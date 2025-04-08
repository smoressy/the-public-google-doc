const path = require('path');
const fs = require('fs');
const app = express();
const sharp = require('sharp');
const { Server } = require('socket.io');
const DiffMatchPatch = require('diff-match-patch');

const dmp = new DiffMatchPatch();

const PORT = process.env.PORT || 3000;
const DOC_FILENAME = 'doc.txt';
const DOC_FILEPATH = path.join(__dirname, DOC_FILENAME);
const SAVE_INTERVAL = 15000;
const CONTENT_DEBOUNCE_TIME = 0; // ms for debouncing content updates
const CURSOR_DEBOUNCE_TIME = 0; // ms for debouncing cursor updates

const CURSOR_TIMEOUT = 120000;
const MAX_DOC_SIZE_MB = 50;
const MAX_IMAGE_UPLOAD_KB = 250;
const IMAGE_MAX_DIMENSION = 400;
const IMAGE_JPEG_QUALITY = 40;

const server = app.listen(PORT, () => {
    console.log(`Server running on port ${PORT}`);
    console.log(`Document will be saved to: ${DOC_FILEPATH}`);
});

const io = new Server(server, {
    maxHttpBufferSize: 2 * 1024 * 1024,
    pingInterval: 10000,
    pingTimeout: 5000,
    transports: ['websocket', 'polling']
});

let documentContent = "<h1>Optimized Editor</h1><p>Start typing! Images capped at 250KB & heavily compressed.</p>";
let connectedUsers = {};
let isSavingToFile = false;
let saveDebounceTimer = null;

function loadDocument() {
    try {
        if (fs.existsSync(DOC_FILEPATH)) {
            const stats = fs.statSync(DOC_FILEPATH);
            const fileSizeInMB = stats.size / (1024 * 1024);

            if (fileSizeInMB > MAX_DOC_SIZE_MB) {
                console.warn(`Document file ${DOC_FILENAME} (${fileSizeInMB.toFixed(2)} MB) exceeds max size (${MAX_DOC_SIZE_MB} MB). Starting with default content.`);
                documentContent = `<h1>Error Loading Document</h1><p>The document file was too large (>${MAX_DOC_SIZE_MB}MB) to load. Starting fresh.</p>`;
                try {
                    fs.writeFileSync(DOC_FILEPATH, documentContent, 'utf8');
                    console.log(`Overwrote large file ${DOC_FILENAME} with error message.`);
                } catch (overwriteErr) {
                    console.error(`Failed to overwrite large file ${DOC_FILENAME}:`, overwriteErr);
                }
            } else {
                documentContent = fs.readFileSync(DOC_FILEPATH, 'utf8');
                console.log(`Document loaded from ${DOC_FILENAME} (${fileSizeInMB.toFixed(2)} MB)`);
            }
        } else {
            console.log(`${DOC_FILENAME} not found. Starting with default content and creating file.`);
            saveDocumentSync();
        }
    } catch (err) {
        console.error('Error loading document:', err);
        if (err.code === 'ENOENT') {
            console.log('File not found during load, proceeding with default content.');
            saveDocumentSync();
        } else {
            console.error(`Unexpected error loading ${DOC_FILENAME}. Starting with default content.`);
            documentContent = "<h1>Error Loading Document</h1><p>An unexpected error occurred while loading the document. Starting fresh.</p>";
        }
    }
}

function saveDocumentSync() {
    if (isSavingToFile) {
        console.warn("Attempted synchronous save while another save was in progress. Skipping sync save.");
        return;
    }
    isSavingToFile = true;
    console.log(`Attempting synchronous save to ${DOC_FILENAME}...`);
    try {
        const currentContentSize = Buffer.byteLength(documentContent, 'utf8');
        if (currentContentSize / (1024 * 1024) > MAX_DOC_SIZE_MB) {
             console.error(`Sync save failed: Document content size (${(currentContentSize / (1024 * 1024)).toFixed(2)} MB) exceeds max limit (${MAX_DOC_SIZE_MB} MB). Save aborted.`);
        } else {
            fs.writeFileSync(DOC_FILEPATH, documentContent, 'utf8');
            console.log(`Document saved synchronously to ${DOC_FILENAME}`);
        }
    } catch (err) {
        console.error('Error saving document synchronously:', err);
        if (err.code === 'ERR_FS_FILE_TOO_LARGE') {
            console.error(`Sync save failed: File system reported too large despite pre-check. Content might be lost.`);
        }
    } finally {
        isSavingToFile = false;
    }
}
function saveDocumentAsync() {
    clearTimeout(saveDebounceTimer);
    saveDebounceTimer = setTimeout(() => {
        if (isSavingToFile) {
            return;
        }
        isSavingToFile = true;

        const currentContentSize = Buffer.byteLength(documentContent, 'utf8');
        if (currentContentSize / (1024 * 1024) > MAX_DOC_SIZE_MB) {
            console.error(`Async save failed: Document content size (${(currentContentSize / (1024 * 1024)).toFixed(2)} MB) exceeds max limit (${MAX_DOC_SIZE_MB} MB). Save aborted.`);
            isSavingToFile = false;
            return;
        }

        const tempFilePath = DOC_FILEPATH + '.tmp';
        fs.writeFile(tempFilePath, documentContent, 'utf8', (err) => {
            if (err) {
                isSavingToFile = false;
                console.error('Error writing temporary save file:', err);
                try { fs.unlinkSync(tempFilePath); } catch (e) {}
                return;
            }

            fs.rename(tempFilePath, DOC_FILEPATH, (renameErr) => {
                isSavingToFile = false;
                if (renameErr) {
                    console.error('Error renaming temp save file to final:', renameErr);
                     if (renameErr.code === 'ERR_FS_FILE_TOO_LARGE') {
                         console.error(`Async save failed: File system reported too large during rename. Content might be lost.`);
                     }
                     try { fs.unlinkSync(tempFilePath); } catch (e) {}
                } else {
                }
            });
        });
    }, 500);
}

loadDocument();
const saveIntervalId = setInterval(() => {
    saveDocumentAsync();
}, SAVE_INTERVAL);

app.get('/doc', (req, res) => {
    res.send(`
    <!DOCTYPE html>
    <html lang="en">
    <head>
      <meta charset="UTF-8">
      <meta name="viewport" content="width=device-width, initial-scale=1.0">
      <title>The Public Google Doc</title>
      <script src="/socket.io/socket.io.js"></script>
      <link rel="icon" href="https://raw.githubusercontent.com/smoressy/tab-changer-9000/refs/heads/main/GDocs_Favicon_Recreation.png">
      <script src="https://cdnjs.cloudflare.com/ajax/libs/diff_match_patch/20121119/diff_match_patch.js"></script>
      <link rel="preconnect" href="https://fonts.googleapis.com">
      <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
      <link href="https://fonts.googleapis.com/css2?family=Roboto:wght@400;500;700&display=swap" rel="stylesheet">
      <style>
        :root {
          --docs-bg: #F8F9FA; --toolbar-bg: #EDF2FA; --border-color: #DADCE0;
          --text-color: #202124; --icon-color: #5F6368; --button-hover-bg: #E8EAED;
          --button-active-bg: #D2E3FC; --primary-blue: #1A73E8; --avatar-border: white;
          --selected-image-outline: #1A73E8; --placeholder-bg: #f0f0f0; --error-red: #D93025;

          --cursor-transition-speed: ${CURSOR_DEBOUNCE_TIME / 1000 * 0.8}s; 
        }
        * { box-sizing: border-box; margin: 0; padding: 0; }
        body { font-family: 'Roboto', Arial, sans-serif; background-color: var(--docs-bg); display: flex; flex-direction: column; height: 100vh; overflow: hidden; }
        #header { display: flex; align-items: center; padding: 6px 16px; border-bottom: 1px solid var(--border-color); background-color: white; flex-shrink: 0; }
        #logo svg { fill: var(--primary-blue); width: 38px; height: 38px; margin-right: 12px;}
        #doc-info { display: flex; flex-direction: column; align-items: flex-start; }
        #title { font-size: 18px; color: var(--text-color); padding: 2px 6px; cursor: default; }
        #menu { display: flex; }
        .menu-item { padding: 4px 8px; color: var(--icon-color); font-size: 14px; cursor: pointer; border-radius: 8px; margin-right: 4px; transition: background-color 0.1s; user-select: none; }
        .menu-item:hover { background-color: var(--button-hover-bg); }
        #header-right { margin-left: auto; display: flex; align-items: center; }
        #active-users { display: flex; align-items: center; margin-right: 16px; min-height: 36px; }
        .user-avatar { width: 32px; height: 32px; border-radius: 50%; color: white; display: flex; align-items: center; justify-content: center; font-weight: 500; font-size: 15px; margin-left: -10px; border: 2px solid var(--avatar-border); cursor: default; transition: transform 0.1s, opacity 0.3s; opacity: 1; }
        .user-avatar:first-child { margin-left: 0; }
        .user-avatar:hover { transform: scale(1.1); z-index: 5; }
        #share-button { background-color: var(--primary-blue); color: white; padding: 8px 16px; border-radius: 32px; font-size: 14px; font-weight: 500; cursor: pointer; transition: background-color 0.1s; user-select: none; }
        #share-button:hover { background-color: #185ABC; }
        #toolbar { display: flex; align-items: center; padding: 4px 8px; background-color: var(--toolbar-bg); border-bottom: 1px solid var(--border-color); overflow-x: auto; flex-shrink: 0; }
        .toolbar-group { display: flex; align-items: center; margin-right: 2px; }
        .toolbar-button { width: 28px; height: 28px; border-radius: 4px; display: flex; align-items: center; justify-content: center; color: var(--icon-color); cursor: pointer; margin: 0 1px; transition: background-color 0.1s; border: 1px solid transparent; }
        .toolbar-button:hover:not(:disabled) { background-color: var(--button-hover-bg); }
        .toolbar-button.active { background-color: var(--button-active-bg); border-color: #ADC8F7; }
        .toolbar-button:disabled { opacity: 0.5; cursor: not-allowed; }
        .toolbar-button svg { width: 18px; height: 18px; fill: currentColor; }
        .separator { width: 1px; height: 20px; background-color: var(--border-color); margin: 0 8px; }
        #content-wrapper { flex: 1; display: flex; justify-content: center; padding: 16px; overflow-y: auto; background-color: var(--docs-bg); position: relative; }
        #editor { width: 816px;  max-width: 100%; min-height: 1056px; background-color: white; box-shadow: 0 1px 3px rgba(0,0,0,0.1), 0 1px 2px rgba(0,0,0,0.08); padding: 72px 90px; outline: none; font-size: 11pt; line-height: 1.5; color: var(--text-color); border: 1px solid var(--border-color); position: relative; margin-bottom: 16px; }
        #editor:focus { outline: none; }
        #editor img { max-width: 100%; height: auto; cursor: pointer; border: 2px solid transparent; border-radius: 2px; vertical-align: middle; }
        #editor img.selected-image { outline: 2px solid var(--selected-image-outline); outline-offset: 2px; border-color: var(--selected-image-outline); }
        #editor .image-placeholder { display: inline-block; width: 100px; height: 80px; background-color: var(--placeholder-bg); border: 1px dashed var(--border-color); text-align: center; line-height: 80px; font-size: 12px; color: var(--icon-color); animation: pulse-placeholder 1.5s infinite ease-in-out; user-select: none; }
        @keyframes pulse-placeholder { 0% { opacity: 1; } 50% { opacity: 0.6; } 100% { opacity: 1; } }
        #status { position: fixed; bottom: 10px; right: 10px; background-color: rgba(248, 249, 250, 0.9); padding: 5px 10px; border-radius: 8px; box-shadow: 0 1px 3px rgba(0,0,0,0.1); font-size: 12px; color: var(--icon-color); z-index: 100; display: flex; align-items: center; }
        #status-indicator { width: 10px; height: 10px; border-radius: 50%; background-color: #1E8E3E; margin-right: 6px; transition: background-color 0.3s; }
        #status-indicator.saving { background-color: #F9AB00; animation: pulse-saving 1.5s infinite ease-in-out; }
        #status-indicator.disconnected { background-color: var(--error-red); }
        #status-indicator.error { background-color: var(--error-red); }
        @keyframes pulse-saving { 0% { opacity: 1; } 50% { opacity: 0.4; } 100% { opacity: 1; } }
        #cursor-container { position: absolute; top: 0; left: 0; width: 100%; height: 100%; pointer-events: none; z-index: 10; overflow: hidden;  }
        .remote-cursor {
             position: absolute;
             width: 2px;
             opacity: 0;

             transition: opacity 0.3s ease, top var(--cursor-transition-speed) linear, left var(--cursor-transition-speed) linear;
             z-index: 1; 
             pointer-events: none;
        }
        .cursor-label { position: absolute; top: -22px; left: -4px; color: white; padding: 2px 5px; border-radius: 8px; font-size: 11px; font-weight: 500; white-space: nowrap; box-shadow: 0 1px 2px rgba(0,0,0,0.2); user-select: none; transform: translateY(0); transition: transform 0.2s ease-out, opacity 0.2s ease-out; opacity: 0; }
        .remote-cursor.visible { opacity: 1; }
        .remote-cursor.visible .cursor-label { opacity: 1; transform: translateY(-2px); }
      </style>
    </head>
    <body>
      <input type="file" id="image-upload-input" accept="image/jpeg,image/png,image/gif" style="display: none;">
      <div id="header">
        <div id="logo">
             <svg focusable="false" viewBox="0 0 72 72"><path d="M66.997 14.043l-1.94-1.94a3.294 3.294 0 00-4.66-.002l-21.09 21.09-5.88-5.88a3.294 3.294 0 00-4.66 0l-16.18 16.18a3.297 3.297 0 000 4.66l1.94 1.94a3.294 3.294 0 004.66.002l21.09-21.09 5.88 5.88a3.294 3.294 0 004.66 0l16.18-16.18a3.297 3.297 0 000-4.66zm-4.66 1.94l-14.24 14.24-5.88-5.88a6.59 6.59 0 00-9.32 0l-21.09 21.09a6.59 6.59 0 000 9.32l1.94 1.94a6.59 6.59 0 009.32 0l21.09-21.09a.75.75 0 01.53-.22c.2 0 .39.07.53.22l5.88 5.88a6.59 6.59 0 009.32 0l14.24-14.24a6.59 6.59 0 000-9.32l-1.94-1.94z"></path></svg>
        </div>
        <div id="doc-info">
            <div id="title">The Public Google Doc</div>
            <div id="menu">
              <div class="menu-item">File</div> <div class="menu-item">Edit</div> <div class="menu-item">View</div> <div class="menu-item">Insert</div> <div class="menu-item">Format</div> <div class="menu-item">Tools</div> <div class="menu-item">Help</div>
            </div>
        </div>
        <div id="header-right">
            <div id="active-users"></div>
            <div id="share-button">Share</div>
        </div>
      </div>
      <div id="toolbar">
        <div class="toolbar-group">
          <button class="toolbar-button" id="btn-bold" title="Bold (Ctrl+B)"><svg viewBox="0 0 24 24"><path d="M15.6 10.8h-2.7v-1.7c0-.8-.3-1.2-.8-1.2h-.1c-.5 0-1.1.4-1.1 1.3v1.6H8.4V7.4c0-.8-.2-1.2-.6-1.2H7.7c-.5 0-.8.4-.8 1.1v3.5H5.6v-.3c0-.8-.3-1.2-.8-1.2H4.7c-.6 0-1 .5-1 1.3v4.5c0 .8.4 1.3 1 1.3h.1c.5 0 .8-.4.8-1.2v-.3h1.3v3.6c0 .8.2 1.2.6 1.2h.1c.4 0 .7-.4.7-1.1V15h2.5v1.8c0 .8.3 1.2.8 1.2h.1c.6 0 1.1-.4 1.1-1.3V15h2.7c1.7 0 2.9-.7 2.9-2.1 0-1.4-1.2-2.1-2.9-2.1zm-8.2 2.9H5.6v-3h1.8v3zm5.9 0h-2.7v-3h2.7c1 0 1.6.4 1.6 1.5 0 1.1-.6 1.5-1.6 1.5z"></path></svg></button>
          <button class="toolbar-button" id="btn-italic" title="Italic (Ctrl+I)"><svg viewBox="0 0 24 24"><path d="M10 5.5h2.1l-3.2 13H6.8l3.2-13zm6.9 0h2.1l-3.2 13h-2.1l3.2-13z"></path></svg></button>
          <button class="toolbar-button" id="btn-underline" title="Underline (Ctrl+U)"><svg viewBox="0 0 24 24"><path d="M12 17.5c3.3 0 6-2.7 6-6V3h-2.5v8.5c0 1.9-1.6 3.5-3.5 3.5s-3.5-1.6-3.5-3.5V3H6v8.5c0 3.3 2.7 6 6 6zM5 19.5v2h14v-2H5z"></path></svg></button>
        </div>
        <div class="separator"></div>
        <div class="toolbar-group">
             <button class="toolbar-button" id="btn-insert-image" title="Insert Image"><svg viewBox="0 0 24 24"><path d="M21 19V5c0-1.1-.9-2-2-2H5c-1.1 0-2 .9-2 2v14c0 1.1.9 2 2 2h14c1.1 0 2-.9 2-2zM8.5 13.5l2.5 3.01L14.5 12l4.5 6H5l3.5-4.5z"></path></svg></button>
        </div>
        <div class="separator"></div>
        <div class="toolbar-group">
             <button class="toolbar-button" id="btn-align-left" title="Align Left"><svg viewBox="0 0 24 24"><path d="M15 15H3v2h12v-2zm0-8H3v2h12V7zM3 13h18v-2H3v2zm0 8h18v-2H3v2zM3 3v2h18V3H3z"></path></svg></button>
             <button class="toolbar-button" id="btn-align-center" title="Align Center"><svg viewBox="0 0 24 24"><path d="M7 15v2h10v-2H7zm-4 6h18v-2H3v2zm0-8h18v-2H3v2zm4-6v2h10V7H7zM3 3v2h18V3H3z"></path></svg></button>
             <button class="toolbar-button" id="btn-align-right" title="Align Right"><svg viewBox="0 0 24 24"><path d="M3 21h18v-2H3v2zm6-4h12v-2H9v2zm-6-8h18v-2H3v2zm6-4h12V7H9v2zM3 3v2h18V3H3z"></path></svg></button>
        </div>
      </div>

      <div id="content-wrapper">
         <!-- Editor container MUST have position:relative for cursor absolute positioning -->
         <div id="editor" contenteditable="true" spellcheck="false" style="position: relative;">
             <!-- Cursor container will be dynamically added here if missing -->
         </div>
      </div>

      <div id="status">
          <div id="status-indicator" title="Connecting..."></div>
          <span id="status-text">Connecting...</span>
      </div>

      <script>

        const socket = io({

        });

        const editor = document.getElementById('editor');
        const statusText = document.getElementById('status-text');
        const statusIndicator = document.getElementById('status-indicator');
        const activeUsersContainer = document.getElementById('active-users');
        const imageUploadInput = document.getElementById('image-upload-input');
        const btnInsertImage = document.getElementById('btn-insert-image');
        const btnBold = document.getElementById('btn-bold');
        const btnItalic = document.getElementById('btn-italic');
        const btnUnderline = document.getElementById('btn-underline');
        const btnAlignLeft = document.getElementById('btn-align-left');
        const btnAlignCenter = document.getElementById('btn-align-center');
        const btnAlignRight = document.getElementById('btn-align-right');

        let cursorContainer; 
        const userId = 'user_' + Math.random().toString(36).substring(2, 11);
        const userColor = \`hsl(\${Math.random() * 360}, 75%, 35%)\`;
        const userName = 'User ' + userId.substring(5, 8).toUpperCase();
        let localUsers = {}; 
        let remoteCursors = {}; 
        let contentDebounceTimer = null;
        let cursorDebounceTimer = null;
        let isEditorFocused = false;
        let currentSelectionRange = null; 
        let isApplyingRemoteUpdate = false; 
        let lastContent = ""; 
        let dmpClient = new diff_match_patch();
        let isCurrentlySaving = false; 
        let selectedImageElement = null; 
        let imageUploadCounter = 0; 

        const CONTENT_DEBOUNCE_TIME_MS = ${CONTENT_DEBOUNCE_TIME};
        const CURSOR_DEBOUNCE_TIME_MS = ${CURSOR_DEBOUNCE_TIME};

        const CURSOR_TIMEOUT_MS = ${CURSOR_TIMEOUT};
        const MAX_IMAGE_SIZE_BYTES = ${MAX_IMAGE_UPLOAD_KB} * 1024;
        const MAX_DOC_SIZE_MB_CLIENT = ${MAX_DOC_SIZE_MB}; 

        socket.on('connect', () => {
          console.log('Connected to server with socket ID:', socket.id);

          Object.keys(remoteCursors).forEach(removeRemoteCursor);
          remoteCursors = {};
          localUsers = {};
          activeUsersContainer.innerHTML = '';
          lastContent = "";
          selectedImageElement = null;
          isApplyingRemoteUpdate = false;
          isCurrentlySaving = false;

          socket.emit('userJoined', { userId, name: userName, color: userColor });
          updateStatus('Requesting document...', false);

        });

        socket.on('init', (data) => {
          console.log('Received init data. Users:', data.users);
          isApplyingRemoteUpdate = true; 

          const hadFocus = document.activeElement === editor;
          const currentScroll = { top: editor.scrollTop, left: editor.scrollLeft };

          editor.innerHTML = data.content || '';

          cursorContainer = editor.querySelector('#cursor-container');
          if (!cursorContainer) {
              console.warn("Cursor container missing in initial content, creating...");
              cursorContainer = document.createElement('div');
              cursorContainer.id = 'cursor-container';
              editor.appendChild(cursorContainer); 
          }
          cursorContainer.innerHTML = ''; 

          lastContent = getSanitizedEditorContent(); 

          remoteCursors = {};
          selectedImageElement = null;
          localUsers = {};

          localUsers[userId] = { name: userName, color: userColor };

          if (data.users) {
             Object.keys(data.users).forEach(otherUserId => {

                 if (otherUserId !== userId && data.users[otherUserId]) {
                    localUsers[otherUserId] = data.users[otherUserId];
                 }
             });
          }
          updateActiveUsersUI();

          if (hadFocus) editor.focus();
          editor.scrollTop = currentScroll.top;
          editor.scrollLeft = currentScroll.left;
          updateToolbarStates(); 

          updateStatus('Connected & Synced', false);
          isApplyingRemoteUpdate = false; 
          console.log('Initialization complete. Local user:', userId);
        });

        socket.on('disconnect', (reason) => {
          console.error('Disconnected from server:', reason);
          updateStatus('Disconnected', false, true);
          activeUsersContainer.innerHTML = '<span style="color: var(--error-red); font-size: 12px; padding-left: 10px;">Offline</span>';

          Object.keys(remoteCursors).forEach(removeRemoteCursor);
          remoteCursors = {};

          if (localUsers[userId]) {
             localUsers = { [userId]: localUsers[userId] }; 
          } else {
             localUsers = {};
          }
        });

        socket.on('connect_error', (err) => {
            console.error('Connection error:', err.message);
            updateStatus('Connection Failed', false, true);
        });

        socket.on('error', (err) => {
           console.error("Socket error:", err);
           updateStatus('Error', false, true);
        });

        socket.on('applyPatch', (patchData) => {

            if (!patchData || !patchData.patch || patchData.senderId === userId) {
                return;
            }

            console.log('Received remote patch from:', patchData.senderId);
            isApplyingRemoteUpdate = true; 

            const selection = window.getSelection();
            let savedRangeData = null;

            if (selection.rangeCount > 0 && editor.contains(selection.anchorNode)) {
                savedRangeData = saveSelection(selection.getRangeAt(0));
            }
            const currentScroll = { top: editor.scrollTop, left: editor.scrollLeft };
            const selectedImgId = selectedImageElement?.id; 

            try {
                const results = dmpClient.patch_apply(patchData.patch, lastContent);
                const patchedContent = results[0];
                const applySuccess = results[1].every(Boolean);

                if (applySuccess && patchedContent !== lastContent) {

                    if (!ensureCursorContainer()) {
                         console.error("CRITICAL: Failed to ensure cursor container before patch apply. Requesting sync.");
                         isApplyingRemoteUpdate = false; 
                         requestFullSync("Cursor container lost during patch apply");
                         return;
                    }

                    const existingCursors = Array.from(cursorContainer.children);
                    existingCursors.forEach(c => c.remove());

                    editor.innerHTML = patchedContent;

                    cursorContainer = editor.querySelector('#cursor-container');
                    if (!cursorContainer) {
                        console.warn("Cursor container removed by patch application, recreating...");
                        cursorContainer = document.createElement('div');
                        cursorContainer.id = 'cursor-container';
                        editor.appendChild(cursorContainer);
                    }
                    existingCursors.forEach(c => cursorContainer.appendChild(c)); 

                    lastContent = getSanitizedEditorContent();

                    editor.scrollTop = currentScroll.top;
                    editor.scrollLeft = currentScroll.left;

                    if (selectedImgId) {
                        const potentiallyRestoredImage = editor.querySelector(\`img#\${selectedImgId}\`);
                        if (potentiallyRestoredImage) {
                            selectImage(potentiallyRestoredImage); 
                        } else {
                           restoreSelection(savedRangeData); 
                        }
                    } else {
                        restoreSelection(savedRangeData); 
                    }

                    updateToolbarStates(); 

                } else if (!applySuccess) {
                    console.warn("Patch application failed. Requesting full sync.", results[1]);

                    requestFullSync("Patch apply failed");
                } else {

                     lastContent = patchedContent; 

                     editor.scrollTop = currentScroll.top; editor.scrollLeft = currentScroll.left;
                     restoreSelection(savedRangeData);
                     updateToolbarStates();
                }

            } catch (error) {
                console.error("Error applying patch:", error);
                requestFullSync("Exception during patch apply");
            } finally {
                isApplyingRemoteUpdate = false; 
            }
        });

        socket.on('contentAcknowledged', () => {

            if (isCurrentlySaving) {
                 updateStatus('Saved', false); 
            }
            isCurrentlySaving = false;
        });

        socket.on('patchRejected', (data) => {
            console.error("Server rejected patch:", data?.reason || "Unknown reason");
            updateStatus(\`Error: \${data?.reason || 'Change rejected'}\`, false, true);
            isCurrentlySaving = false; 

            requestFullSync("Local patch rejected by server");
        });

        socket.on('requestFullSync', (data) => {
            console.warn("Server requested full sync. Reason:", data?.reason || "N/A");
            updateStatus("Syncing...", true);

        });

        editor.addEventListener('input', (event) => {

            if (isApplyingRemoteUpdate || !event.isTrusted) {
                return;
            }

            if (!isCurrentlySaving) {
                updateStatus('Saving', true);
            }
            isCurrentlySaving = true;

            clearTimeout(contentDebounceTimer);
            contentDebounceTimer = setTimeout(() => {
                sendPatch(); 
            }, CONTENT_DEBOUNCE_TIME_MS); 

            updateToolbarStates();
            sendCursorPositionDebounced(); 
        });

        function sendPatch() {
            if (isApplyingRemoteUpdate) return; 

            const currentContent = getSanitizedEditorContent();

             const approxSizeMB = (currentContent.length * 2) / (1024 * 1024); 
             if (approxSizeMB > MAX_DOC_SIZE_MB_CLIENT * 1.1) { 
                 updateStatus(\`Error: Content too large (>\${MAX_DOC_SIZE_MB_CLIENT}MB)\`, false, true);

                 console.error("Local content exceeds size limit, preventing patch send.");
                 return;
             }

            if (currentContent === lastContent) {

                 if (isCurrentlySaving) {

                 }
                return;
            }

            try {
                const patch = dmpClient.patch_make(lastContent, currentContent);

                if (patch.length === 0 && currentContent !== lastContent) {
                     console.warn("Patch is empty but content differs? Updating lastContent.");
                     lastContent = currentContent; 
                     if (isCurrentlySaving) {

                     }
                    return;
                } else if (patch.length === 0) {

                    lastContent = currentContent;
                    if (isCurrentlySaving) {

                    }
                    return;
                }

                socket.emit('applyPatch', { patch });
                lastContent = currentContent; 

            } catch (error) {
                console.error("Error creating patch:", error);
                 requestFullSync("Local patch creation failed"); 

            }
        }

        function updateLastContentAndSendPatch() {
             console.log("Forcing content update and patch send.");
             clearTimeout(contentDebounceTimer); 

             sendPatch(); 
        }

        function requestFullSync(reason) {
             console.warn("Requesting full sync from server. Reason:", reason);
             updateStatus("Syncing...", true);
             socket.emit('requestFullSync', { reason }); 
        }

        document.addEventListener('selectionchange', sendCursorPositionDebounced);
        editor.addEventListener('keyup', sendCursorPositionDebounced); 
        editor.addEventListener('mouseup', sendCursorPositionDebounced); 
        editor.addEventListener('focus', () => {
            isEditorFocused = true;
            updateToolbarStates();
            sendCursorPosition(); 
        });
        editor.addEventListener('blur', () => {
            isEditorFocused = false;

        });

        function sendCursorPositionDebounced() {

             const selection = window.getSelection();
             const shouldSend = isEditorFocused ||
                               (selection && selection.rangeCount > 0 && editor.contains(selection.anchorNode)) ||
                               (selectedImageElement && editor.contains(selectedImageElement));

             if (!shouldSend || isApplyingRemoteUpdate) {
                 return; 
             }

             clearTimeout(cursorDebounceTimer);
             cursorDebounceTimer = setTimeout(sendCursorPosition, CURSOR_DEBOUNCE_TIME_MS); 
         }

        function sendCursorPosition() {

             if (!ensureCursorContainer()) {
                 console.warn("Cannot send cursor position, container invalid.");
                 return;
             }

             const selection = window.getSelection();
             let cursorData = { userId }; 

             try {
                 const containerRect = cursorContainer.getBoundingClientRect();
                 if (!containerRect || containerRect.width === 0 || containerRect.height === 0) {
                      console.warn("Cursor container rect is invalid, cannot calculate position accurately.");

                      return; 
                 }

                 if (selectedImageElement && editor.contains(selectedImageElement)) {

                     const imgRect = selectedImageElement.getBoundingClientRect();
                     cursorData.x = imgRect.left - containerRect.left;
                     cursorData.y = imgRect.top - containerRect.top;
                     cursorData.height = imgRect.height;
                     cursorData.isImage = true;

                 } else if (selection && selection.rangeCount > 0 && editor.contains(selection.anchorNode)) {
                     const range = selection.getRangeAt(0);
                     currentSelectionRange = range.cloneRange(); 
                     updateToolbarStates(); 

                     let rect;

                     if (range.collapsed) {

                         const tempSpan = document.createElement('span');
                         tempSpan.textContent = '\\ufeff';
                         range.insertNode(tempSpan);
                         rect = tempSpan.getBoundingClientRect();
                         tempSpan.remove(); 
                     } else {

                         rect = range.getBoundingClientRect();
                     }

                     if (!rect || (rect.width === 0 && rect.height === 0 && range.collapsed)) {

                         const clientRects = range.getClientRects();
                         if (clientRects.length > 0) {
                            rect = clientRects[0];
                         } else {

                             console.warn("Could not get valid rect for caret, using start container.");
                             const startNode = range.startContainer;
                             if (startNode) {
                                 if(startNode.nodeType === Node.ELEMENT_NODE) {
                                     rect = startNode.getBoundingClientRect();
                                 } else if (startNode.parentElement) {

                                     rect = startNode.parentElement.getBoundingClientRect();
                                 }
                             }
                         }
                     }

                     if (!rect || (rect.width === 0 && rect.height === 0)) {
                         console.warn("Fallback rect failed, cannot determine cursor position reliably.");
                         return; 
                     }

                     cursorData.x = rect.left - containerRect.left;
                     cursorData.y = rect.top - containerRect.top;
                     cursorData.height = rect.height || 18; 
                     cursorData.isImage = false;

                 } else {

                     return;
                 }

                cursorData.x = Math.max(0, cursorData.x);
                cursorData.y = Math.max(0, cursorData.y);

                socket.emit('cursorMove', cursorData);

             } catch (error) {
                console.error("Error calculating cursor position:", error);

             }
         }

         socket.on('cursorMove', (data) => {

            if (data.userId === userId || isApplyingRemoteUpdate) return;
            const user = localUsers[data.userId];
            if (!user) {

               return; 
            }

            updateRemoteCursor({ ...data, name: user.name, color: user.color });
        });

        function updateRemoteCursor(data) {

            if (!ensureCursorContainer()) return;

            let cursorInfo = remoteCursors[data.userId];

            if (!cursorInfo || !cursorContainer.contains(cursorInfo.element)) {
                if (cursorInfo && cursorInfo.element) cursorInfo.element.remove(); 

                const cursorEl = document.createElement('div');
                cursorEl.className = 'remote-cursor';
                cursorEl.style.backgroundColor = data.color || '#CCCCCC'; 
                cursorEl.dataset.userId = data.userId;

                const labelEl = document.createElement('div');
                labelEl.className = 'cursor-label';
                labelEl.textContent = data.name || 'User'; 
                labelEl.style.backgroundColor = data.color || '#CCCCCC';

                cursorEl.appendChild(labelEl);
                cursorContainer.appendChild(cursorEl); 

                cursorInfo = { element: cursorEl, timeoutId: null };
                remoteCursors[data.userId] = cursorInfo; 
            }

            const cursorEl = cursorInfo.element;
            cursorEl.style.left = \`\${Math.max(0, data.x)}px\`; 
            cursorEl.style.top = \`\${Math.max(0, data.y)}px\`;
            cursorEl.style.height = \`\${data.height || 18}px\`;
            cursorEl.classList.add('visible'); 

            clearTimeout(cursorInfo.timeoutId);
            cursorInfo.timeoutId = setTimeout(() => {
                if (cursorInfo.element) {
                    cursorInfo.element.classList.remove('visible'); 
                }
            }, CURSOR_TIMEOUT_MS);
        }

        function removeRemoteCursor(id) {
            if (remoteCursors[id]) {
                clearTimeout(remoteCursors[id].timeoutId); 
                if (remoteCursors[id].element) {
                    remoteCursors[id].element.remove(); 
                }
                delete remoteCursors[id]; 
            }
        }

        socket.on('userJoined', (data) => {

            if (data.userId === userId || localUsers[data.userId]) return;
            console.log('User joined:', data.name, data.userId);
            localUsers[data.userId] = { name: data.name, color: data.color }; 
            updateActiveUsersUI(); 
        });

        socket.on('userLeft', (data) => {
            if (!localUsers[data.userId]) return; 
            console.log('User left:', localUsers[data.userId]?.name || data.userId);
            removeRemoteCursor(data.userId); 
            delete localUsers[data.userId]; 
            updateActiveUsersUI(); 
        });

        function updateActiveUsersUI() {
          if (!activeUsersContainer) return;
          activeUsersContainer.innerHTML = ''; 
          const userIds = Object.keys(localUsers);

          userIds.forEach(id => {
            const user = localUsers[id];

            if (!user || typeof user.name !== 'string' || typeof user.color !== 'string') return;

            const avatar = document.createElement('div');
            avatar.className = 'user-avatar';
            avatar.style.backgroundColor = user.color;

            avatar.textContent = user.name.substring(0, 1).toUpperCase() || '?';
            avatar.title = user.name + (id === userId ? ' (You)' : ''); 
            avatar.dataset.userId = id; 
            activeUsersContainer.appendChild(avatar);
          });
        }

        btnInsertImage.addEventListener('click', () => {
            imageUploadInput.click(); 
        });

        imageUploadInput.addEventListener('change', (event) => {
            const file = event.target.files[0];
            imageUploadInput.value = ''; 

            if (!file) return;

            if (file.size > MAX_IMAGE_SIZE_BYTES) {
                alert(\`Image too large! Max size: \${MAX_IMAGE_UPLOAD_KB}KB.\`);
                updateStatus(\`Error: Image > \${MAX_IMAGE_UPLOAD_KB}KB\`, false, true);

                setTimeout(() => updateStatus(isCurrentlySaving ? 'Saving...' : 'Saved', isCurrentlySaving, socket.connected ? false : true), 3000);
                return;
            }

            insertImagePlaceholderAndUpload(file); 
        });

        editor.addEventListener('paste', (event) => {
            const items = (event.clipboardData || window.clipboardData)?.items;
            if (!items) return;

            let foundImage = false;
            for (let i = 0; i < items.length; i++) {
                if (items[i].type.startsWith('image/')) {
                    const file = items[i].getAsFile();
                    if (!file) continue; 

                    if (file.size > MAX_IMAGE_SIZE_BYTES) {
                        alert(\`Pasted image too large! Max size: \${MAX_IMAGE_UPLOAD_KB}KB.\`);
                        updateStatus(\`Error: Image > \${MAX_IMAGE_UPLOAD_KB}KB\`, false, true);
                         setTimeout(() => updateStatus(isCurrentlySaving ? 'Saving...' : 'Saved', isCurrentlySaving, socket.connected ? false : true), 3000);
                         event.preventDefault(); 
                        return;
                    }

                    foundImage = true;
                    event.preventDefault(); 
                    insertImagePlaceholderAndUpload(file);
                    break; 
                }
            }

        });

        function insertImagePlaceholderAndUpload(file) {
            const selection = window.getSelection();

            if (!selection.rangeCount) {
                placeCursorAtStart(); 
                if (!selection.rangeCount) return; 
            };

            deselectImage(); 

            const range = selection.getRangeAt(0);
            range.deleteContents(); 

            const placeholderId = \`placeholder_\${Date.now()}_\${++imageUploadCounter}\`; 
            const placeholder = document.createElement('span');
            placeholder.className = 'image-placeholder';
            placeholder.dataset.placeholderId = placeholderId;
            placeholder.textContent = 'Uploading...';
            placeholder.contentEditable = false; 

            range.insertNode(placeholder); 

            range.setStartAfter(placeholder);
            range.collapse(true);
            selection.removeAllRanges();
            selection.addRange(range);

            console.log('Inserted placeholder:', placeholderId);
            editor.focus(); 
            updateLastContentAndSendPatch(); 

            const reader = new FileReader();
            reader.onload = (e) => {
                const base64Data = e.target.result;
                if (!base64Data) {
                    console.error("FileReader error: result is null for", placeholderId);
                    placeholder.textContent = 'Read Error';
                    placeholder.style.animation = 'none';
                    placeholder.style.backgroundColor = '#ffebee';
                    updateStatus('Error reading image', false, true);
                    setTimeout(() => placeholder.remove(), 5000);
                    updateLastContentAndSendPatch();
                    return;
                }
                console.log(\`Sending image (\${(file.size / 1024).toFixed(1)}KB) for placeholder \${placeholderId} to server...\`);
                socket.emit('uploadImage', { placeholderId, base64Data });

                updateStatus('Uploading Image...', true);
            };
            reader.onerror = (e) => {
                console.error("Error reading file:", e);
                placeholder.textContent = 'Read Error';
                 placeholder.style.animation = 'none';
                 placeholder.style.backgroundColor = '#ffebee';
                 updateStatus('Error reading image', false, true);

                 setTimeout(() => placeholder.remove(), 5000);
                 updateLastContentAndSendPatch();
            };
            reader.readAsDataURL(file);
        }

        socket.on('imageProcessed', ({ placeholderId, optimizedBase64, error }) => {
            const placeholder = editor.querySelector(\`.image-placeholder[data-placeholder-id="\${placeholderId}"]\`);
            if (!placeholder) {

                if (statusText.textContent === 'Uploading Image...') {
                    updateStatus(isCurrentlySaving ? 'Saving...' : 'Saved', isCurrentlySaving, socket.connected ? false : true);
                }
                return;
            }

            if (error) {
                console.error("Server failed to process image:", placeholderId, error);
                placeholder.textContent = 'Upload Failed';
                placeholder.style.animation = 'none';
                placeholder.style.backgroundColor = '#ffebee'; 
                updateStatus(\`Error: \${error}\`, false, true); 

                setTimeout(() => {
                    placeholder.remove();
                    updateLastContentAndSendPatch();

                    updateStatus(isCurrentlySaving ? 'Saving...' : 'Saved', isCurrentlySaving, socket.connected ? false : true);
                }, 5000);
                return;
            }

            console.log("Received processed image for placeholder:", placeholderId);
            const img = document.createElement('img');
            img.src = optimizedBase64;

            img.id = 'img_' + Date.now() + '_' + Math.random().toString(36).substring(2, 7);
            img.contentEditable = false; 

            try {

                 placeholder.replaceWith(img);
                 updateLastContentAndSendPatch(); 

                 updateStatus(isCurrentlySaving ? 'Saving...' : 'Saved', isCurrentlySaving, socket.connected ? false : true);

            } catch (e) {
                console.error("Error replacing placeholder with image:", e);

                 try {
                     const parent = placeholder.parentNode;
                     const nextSibling = placeholder.nextSibling;
                     placeholder.remove();
                     if (parent) {
                         parent.insertBefore(img, nextSibling);
                         updateLastContentAndSendPatch(); 
                         updateStatus(isCurrentlySaving ? 'Saving...' : 'Saved', isCurrentlySaving, socket.connected ? false : true);
                     } else {
                         throw new Error("Placeholder parent not found for fallback insertion.");
                     }
                 } catch (e2) {
                    console.error("Fallback image insertion failed:", e2);
                    updateStatus('Error inserting image', false, true);

                 }
            }
        });

        editor.addEventListener('click', handleEditorClick);
        document.addEventListener('keydown', handleKeyDown); 

        function handleEditorClick(event) {
            const target = event.target;
            if (target.tagName === 'IMG' && editor.contains(target)) {

                selectImage(target);
                 event.preventDefault(); 
                 event.stopPropagation(); 
            } else if (selectedImageElement && (!target || !editor.contains(target))) {

                deselectImage();
            } else if (selectedImageElement && target !== selectedImageElement && editor.contains(target)) {

                 deselectImage();
            }

        }

        function selectImage(imgElement) {
            if (selectedImageElement === imgElement) return; 

            deselectImage(); 

            selectedImageElement = imgElement;
            selectedImageElement.classList.add('selected-image'); 
            console.log("Selected image:", selectedImageElement.id);

             const selection = window.getSelection();
             const range = document.createRange();
             try {
                 range.selectNode(selectedImageElement); 
                 selection.removeAllRanges(); 
                 selection.addRange(range); 
                 currentSelectionRange = range.cloneRange(); 
                 updateToolbarStates(); 
                 sendCursorPosition(); 
             } catch(e) {
                 console.error("Error selecting image node:", e);
                 deselectImage(); 
             }
        }

        function deselectImage() {
            if (selectedImageElement) {
                selectedImageElement.classList.remove('selected-image'); 
                console.log("Deselected image:", selectedImageElement.id);
                selectedImageElement = null;

                const selection = window.getSelection();
                if (selection.rangeCount > 0) {
                    const range = selection.getRangeAt(0);
                    if(range.startContainer === editor && range.endContainer === editor && range.startOffset === range.endOffset) {

                    }
                }

            }
        }

        function handleKeyDown(event) {

            if (!selectedImageElement || !editor.contains(selectedImageElement)) return;

            if (event.key === 'Delete' || event.key === 'Backspace') {
                event.preventDefault(); 
                console.log('Delete/Backspace pressed for selected image:', selectedImageElement.id);

                const imageToRemove = selectedImageElement; 

                deselectImage();

                const range = document.createRange();
                const selection = window.getSelection();
                 try {

                    range.setStartBefore(imageToRemove);
                    range.collapse(true); 
                    selection.removeAllRanges();
                    selection.addRange(range);
                    currentSelectionRange = range.cloneRange(); 
                 } catch(e){
                    console.warn("Could not set range before image removal, fallback placement might occur.", e);
                    placeCursorAtStart(); 
                 }

                imageToRemove.remove();

                updateLastContentAndSendPatch();
                editor.focus(); 

            } else if (['ArrowLeft', 'ArrowRight', 'ArrowUp', 'ArrowDown'].includes(event.key)) {

                 setTimeout(() => {
                     const selection = window.getSelection();

                     if (!selection.rangeCount || !selectedImageElement) {
                        if (selectedImageElement) deselectImage(); 
                        return;
                     }
                     const range = selection.getRangeAt(0);

                     if (!range.intersectsNode(selectedImageElement) && !(range.startContainer === selectedImageElement || range.endContainer === selectedImageElement)) {
                         deselectImage();
                     }
                 }, 0);
            }
        }

        function applyFormat(command, value = null) {

            if (selectedImageElement && ['bold', 'italic', 'underline'].includes(command)) {
                return;
            }

            document.execCommand(command, false, value);
            editor.focus(); 
            updateToolbarStates(); 

            setTimeout(updateLastContentAndSendPatch, 10);
        }

        btnBold.addEventListener('click', () => applyFormat('bold'));
        btnItalic.addEventListener('click', () => applyFormat('italic'));
        btnUnderline.addEventListener('click', () => applyFormat('underline'));
        btnAlignLeft.addEventListener('click', () => applyFormat('justifyLeft'));
        btnAlignCenter.addEventListener('click', () => applyFormat('justifyCenter'));
        btnAlignRight.addEventListener('click', () => applyFormat('justifyRight'));

        function updateToolbarStates() {

             if (!isEditorFocused && !currentSelectionRange && !selectedImageElement) return;

             try {
                 const isImageSelected = selectedImageElement && editor.contains(selectedImageElement);

                 btnBold.disabled = isImageSelected;
                 btnItalic.disabled = isImageSelected;
                 btnUnderline.disabled = isImageSelected;

                 if (!isImageSelected) {

                    btnBold.classList.toggle('active', document.queryCommandState('bold'));
                    btnItalic.classList.toggle('active', document.queryCommandState('italic'));
                    btnUnderline.classList.toggle('active', document.queryCommandState('underline'));
                 } else {

                     btnBold.classList.remove('active');
                     btnItalic.classList.remove('active');
                     btnUnderline.classList.remove('active');
                 }

                 btnAlignLeft.classList.toggle('active', document.queryCommandState('justifyLeft'));
                 btnAlignCenter.classList.toggle('active', document.queryCommandState('justifyCenter'));
                 btnAlignRight.classList.toggle('active', document.queryCommandState('justifyRight'));

             } catch (e) { console.warn("Error querying command state:", e); }
        }

        function getSanitizedEditorContent() {

            const clonedEditor = editor.cloneNode(true);

            const tempCursorContainer = clonedEditor.querySelector('#cursor-container');
            if (tempCursorContainer) {
                tempCursorContainer.remove();
            }

            const tempSelectedImage = clonedEditor.querySelector('.selected-image');
             if (tempSelectedImage) {
                 tempSelectedImage.classList.remove('selected-image');
             }
            return clonedEditor.innerHTML;
        }

        function ensureCursorContainer() {

            if (!cursorContainer || !editor.contains(cursorContainer)) {
                cursorContainer = editor.querySelector('#cursor-container'); 
                if (!cursorContainer) {
                    console.error("CRITICAL: Cursor container lost! Recreating.");
                    cursorContainer = document.createElement('div');
                    cursorContainer.id = 'cursor-container';

                    editor.appendChild(cursorContainer);

                    Object.values(remoteCursors).forEach(cursorInfo => {
                        if (cursorInfo.element && !cursorContainer.contains(cursorInfo.element)) {
                           cursorContainer.appendChild(cursorInfo.element);
                        }
                    });

                    if (!editor.contains(cursorContainer)) {
                        console.error("FATAL: Failed to re-append cursor container!");
                        return false; 
                    }
                }
            }
            return true; 
        }

        function updateStatus(message, isSavingFlag = false, isError = false) {

            const currentStatus = statusText.textContent;
            if ((currentStatus === 'Syncing...' || statusIndicator.classList.contains('disconnected') || statusIndicator.classList.contains('error')) && !isError && message !== 'Connected & Synced') {

               if(message === 'Connected & Synced') {

               } else {

                   return;
               }
            }

            isCurrentlySaving = isSavingFlag; 
            statusText.textContent = message;
            statusIndicator.classList.remove('saving', 'disconnected', 'error'); 
            statusIndicator.title = ''; 

            if (isError) {
                 statusIndicator.classList.add('error');
                 statusIndicator.title = message; 
            } else if (isSavingFlag) {
                statusIndicator.classList.add('saving');
                statusIndicator.title = 'Saving changes...';
            } else if (message === 'Disconnected' || message === 'Connection Failed') {
                 statusIndicator.classList.add('disconnected');
                 statusIndicator.title = message === 'Disconnected' ? 'Connection lost' : 'Could not connect';
            } else {

                 statusIndicator.classList.remove('saving', 'disconnected', 'error');
                 statusIndicator.style.backgroundColor = '#1E8E3E'; 
                 statusIndicator.title = 'Document synced';
            }
        }

        function getNodePath(node) {
            const path = [];
            let targetNode = node;
            while (targetNode && targetNode !== editor) {
                 const parent = targetNode.parentNode;
                 if (!parent) return null; 

                 if (targetNode.id === 'cursor-container') {
                     targetNode = parent; continue;
                 }

                 let index = 0;
                 let sibling = targetNode.previousSibling;
                 while (sibling) {

                     if (sibling.id !== 'cursor-container' && (sibling.nodeType === Node.ELEMENT_NODE || sibling.nodeType === Node.TEXT_NODE)) {
                         index++;
                     }
                     sibling = sibling.previousSibling;
                 }
                 path.unshift(index); 
                 targetNode = parent; 
            }

            return (targetNode === editor || node === editor) ? path : null;
        }

        function getNodeByPath(parent, path) {
             if (!path) return null;
             let node = parent;
             try {
                 for (const index of path) {
                     if (!node || !node.childNodes) return null; 

                     let count = 0;
                     let foundNode = null;

                     for(let i = 0; i < node.childNodes.length; i++) {
                         const child = node.childNodes[i];

                         if (child.id === 'cursor-container' || (child.nodeType !== Node.ELEMENT_NODE && child.nodeType !== Node.TEXT_NODE)) {
                             continue;
                         }

                         if (count === index) {
                            foundNode = child;
                            break;
                         }
                         count++; 
                     }

                     if (!foundNode) {

                         return null;
                     }
                     node = foundNode; 
                 }
                 return node; 
             } catch (e) {
                 console.error("Error getting node by path:", path, e);
                 return null;
             }
        }

        function saveSelection(range) {

            if (!range || !editor.contains(range.commonAncestorContainer)) return null;
            try {

                 const startPath = getNodePath(range.startContainer);
                 const endPath = getNodePath(range.endContainer);

                 if (startPath === null || endPath === null) {
                     console.warn("Could not generate valid node path for selection.");
                     return null;
                 }

                 return {
                    startContainerPath: startPath,
                    startOffset: range.startOffset,
                    endContainerPath: endPath,
                    endOffset: range.endOffset,
                    collapsed: range.collapsed
                };
            } catch (e) {
                console.warn("Error saving selection:", e);
                return null;
            }
        }

        function restoreSelection(savedData) {

            if (!savedData || !savedData.startContainerPath || !savedData.endContainerPath) {

                 return;
             }

            try {
                const selection = window.getSelection();
                selection.removeAllRanges(); 

                const newStartContainer = getNodeByPath(editor, savedData.startContainerPath);
                const newEndContainer = getNodeByPath(editor, savedData.endContainerPath);

                if (newStartContainer && newEndContainer) {
                    const newRange = document.createRange();

                    const startLength = (newStartContainer.nodeType === Node.TEXT_NODE) ? newStartContainer.nodeValue.length : newStartContainer.childNodes.length;
                    const endLength = (newEndContainer.nodeType === Node.TEXT_NODE) ? newEndContainer.nodeValue.length : newEndContainer.childNodes.length;

                    const validStartOffset = Math.min(savedData.startOffset, startLength);
                    const validEndOffset = Math.min(savedData.endOffset, endLength);

                    if (typeof validStartOffset !== 'number' || typeof validEndOffset !== 'number') {
                    }

                    newRange.setStart(newStartContainer, validStartOffset);
                    newRange.setEnd(newEndContainer, validEndOffset);

                    selection.addRange(newRange);
                    currentSelectionRange = newRange.cloneRange(); 

                } else {
                     console.warn("Couldn't find nodes for saved selection path, placing cursor at start.", savedData);
                     placeCursorAtStart(); 
                }
            } catch (e) {
                console.warn("Couldn't restore precise selection:", e, savedData);
                placeCursorAtStart(); 
            } finally {
                 updateToolbarStates(); 
            }
        }

        function placeCursorAtStart() {
             try {
                 editor.focus(); 
                 const range = document.createRange();
                 const selection = window.getSelection();

                 let firstContentNode = editor.firstChild;
                 while (firstContentNode && (firstContentNode.id === 'cursor-container' || (firstContentNode.nodeType !== Node.ELEMENT_NODE && firstContentNode.nodeType !== Node.TEXT_NODE))) {
                     firstContentNode = firstContentNode.nextSibling;
                 }

                 if (firstContentNode) {

                     if (firstContentNode.nodeType === Node.TEXT_NODE) {
                        range.setStart(firstContentNode, 0);
                     } else {

                         range.setStartBefore(firstContentNode);
                     }
                 } else {

                     range.selectNodeContents(editor);
                 }
                 range.collapse(true); 
                 selection.removeAllRanges(); 
                 selection.addRange(range); 
                 currentSelectionRange = range.cloneRange(); 
                 console.log("Placed cursor at document start as fallback.");
             } catch (focusError) {
                 console.error("Error focusing editor or setting range for fallback selection:", focusError);
             }
         }

      </script>
    </body>
    </html>
  `);
});

io.on('connection', (socket) => {
    console.log(`User connected: ${socket.id}`);

    socket.on('userJoined', (data) => {

         if (!data || typeof data.userId !== 'string' || typeof data.name !== 'string' || typeof data.color !== 'string' || data.userId.length < 5 || data.name.length < 1) {
             console.warn(`Invalid userJoined data from ${socket.id}. Disconnecting. Data:`, data);
             socket.disconnect(true); 
             return;
         }

         const { userId, name, color } = data;

         const existingUser = connectedUsers[userId];
         if (existingUser && existingUser.socketId !== socket.id) {
             console.warn(`User ID ${userId} (${name}) reconnected on new socket ${socket.id}. Disconnecting old socket ${existingUser.socketId}.`);
             const oldSocket = io.sockets.sockets.get(existingUser.socketId);
             if (oldSocket) {

                 oldSocket.disconnect(true);
             }

             delete connectedUsers[userId];
         }

        Object.keys(connectedUsers).forEach(uid => {
            if (connectedUsers[uid].socketId === socket.id && uid !== userId) {
                console.warn(`Socket ${socket.id} was previously user ${uid}, now re-assigned to ${userId}. Removing old entry.`);
                delete connectedUsers[uid];
            }
        });

        console.log(`User identified: ${name} (${userId}) - Socket: ${socket.id}`);
        connectedUsers[userId] = {
            userId: userId,
            name: name,
            color: color,
            socketId: socket.id, 
            lastSeen: Date.now()
        };
         socket.userId = userId; 

        socket.emit('init', {
            content: documentContent,
            users: Object.values(connectedUsers).reduce((acc, user) => {

                 if (user.userId !== userId) {

                     acc[user.userId] = { name: user.name, color: user.color };
                 }
                 return acc;
             }, {})
        });

        socket.broadcast.emit('userJoined', {
            userId: userId,
            name: name,
            color: color
        });
        console.log("Total connected users:", Object.keys(connectedUsers).length);
    });

    socket.on('applyPatch', (data) => {
        const userId = socket.userId; 

        if (!userId || !connectedUsers[userId]) {
             console.warn(`applyPatch received from unknown/unidentified user/socket: ${socket.id}. Ignoring.`);
             return;
        }
         if (!data || !data.patch || !Array.isArray(data.patch)) { 
             console.warn(`Invalid applyPatch data received from ${userId} (${connectedUsers[userId].name}). Data:`, data);

             return;
         }

         try {
             const results = dmp.patch_apply(data.patch, documentContent);
             const newContent = results[0];
             const appliedOK = results[1].every(Boolean); 

             if (appliedOK) {

                 if (newContent !== documentContent) {

                     const newSizeMB = Buffer.byteLength(newContent, 'utf8') / (1024 * 1024);
                     if (newSizeMB > MAX_DOC_SIZE_MB) {
                         console.warn(`Patch from ${userId} rejected: Resulting document size (${newSizeMB.toFixed(2)}MB) exceeds limit (${MAX_DOC_SIZE_MB}MB).`);

                         socket.emit('patchRejected', { reason: `Document size limit (${MAX_DOC_SIZE_MB}MB) exceeded.` });

                         return;
                     }

                     documentContent = newContent;

                     socket.broadcast.emit('applyPatch', { patch: data.patch, senderId: userId });

                     socket.emit('contentAcknowledged');

                     saveDocumentAsync();
                 } else {

                      socket.emit('contentAcknowledged');
                 }
             } else {

                 console.error(`Server failed to apply patch from ${userId} (${connectedUsers[userId].name}). Results:`, results[1], "Patch:", data.patch);

                 socket.emit('requestFullSync', { reason: "Server patch application failed. Please resync." });
             }
         } catch (error) {

              console.error(`Error applying patch from ${userId} (${connectedUsers[userId].name}) on server:`, error);

              socket.emit('requestFullSync', { reason: "Server exception during patch application. Please resync." });
         }
    });

    socket.on('uploadImage', async ({ placeholderId, base64Data }) => {
         const userId = socket.userId; 
        if (!userId || !connectedUsers[userId]) {
             console.warn(`uploadImage received from unknown/unidentified user/socket: ${socket.id}. Ignoring.`);
             return;
        }

        if (!placeholderId || !base64Data) {
             console.warn(`Invalid uploadImage data from ${userId} (${connectedUsers[userId].name}). Placeholder: ${placeholderId}`);

             socket.emit('imageProcessed', { placeholderId, error: "Invalid image upload data received by server." });
             return;
        }

         try {

            if (typeof base64Data !== 'string' || !base64Data.startsWith('data:image/')) {
                throw new Error("Invalid Base64 image format.");
            }

            const matches = base64Data.match(/^data:(image\/(.+));base64,(.*)$/);
            if (!matches || matches.length < 4) {
                 throw new Error("Could not parse Base64 data URL.");
            }
            const mimeType = matches[1];
            const imageData = matches[3];
            const imageBuffer = Buffer.from(imageData, 'base64'); 
            const originalSizeKB = imageBuffer.length / 1024;

            console.log(`Received image from ${userId} (${connectedUsers[userId].name}): Placeholder ${placeholderId}, Type: ${mimeType}, Original Size: ${originalSizeKB.toFixed(1)}KB`);

             if (originalSizeKB > MAX_IMAGE_UPLOAD_KB * 1.05) { 
                 console.warn(`Image from ${userId} rejected: Size (${originalSizeKB.toFixed(1)}KB) exceeds limit (${MAX_IMAGE_UPLOAD_KB}KB) after decoding.`);
                 socket.emit('imageProcessed', { placeholderId, error: `Image too large (>${MAX_IMAGE_UPLOAD_KB}KB)` });
                 return;
             }

             const processedBuffer = await sharp(imageBuffer)
                 .resize({
                     width: IMAGE_MAX_DIMENSION,  
                     height: IMAGE_MAX_DIMENSION, 
                     fit: 'inside',             
                     withoutEnlargement: true   
                 })
                 .jpeg({
                     quality: IMAGE_JPEG_QUALITY, 
                     progressive: true,        

                 })
                 .withMetadata(false) 
                 .toBuffer(); 

             const optimizedBase64 = `data:image/jpeg;base64,${processedBuffer.toString('base64')}`;
             const finalSizeKB = processedBuffer.length / 1024;

             console.log(`Image processed for ${userId}: Placeholder ${placeholderId}, Final Size: ${finalSizeKB.toFixed(1)}KB (JPEG)`);

             socket.emit('imageProcessed', { placeholderId, optimizedBase64 });

         } catch (error) {
             console.error(`Error processing image from ${userId} (Placeholder: ${placeholderId}):`, error);

             socket.emit('imageProcessed', { placeholderId, error: `Server image processing failed: ${error.message || 'Unknown error'}` });
         }
    });

    socket.on('cursorMove', (data) => {
        const userId = socket.userId;

        if (!userId || !connectedUsers[userId]) {

            return;
        }
         if (!data || typeof data.x !== 'number' || typeof data.y !== 'number' || typeof data.height !== 'number') {

             return;
         }

         const user = connectedUsers[userId];
         const cursorUpdateData = {
             ...data, 
             userId: userId,
             name: user.name,
             color: user.color
         };

         user.lastSeen = Date.now();

         socket.broadcast.emit('cursorMove', cursorUpdateData);
    });

    socket.on('requestFullSync', (data) => {
         const userId = socket.userId;
         const reason = data?.reason || 'N/A';
         console.log(`User ${userId || socket.id} requested full sync. Reason: ${reason}`);

          socket.emit('init', {
            content: documentContent,
             users: Object.values(connectedUsers).reduce((acc, user) => {

                 if (user.userId !== userId) {
                     acc[user.userId] = { name: user.name, color: user.color };
                 }
                 return acc;
             }, {})
         });

         socket.emit('contentAcknowledged');
    });

    socket.on('disconnect', (reason) => {
        const userIdLeaving = socket.userId; 
        console.log(`User disconnected: Socket ${socket.id}, UserID: ${userIdLeaving || 'N/A'}, Reason: ${reason}`);

        if (userIdLeaving && connectedUsers[userIdLeaving]) {

            if (connectedUsers[userIdLeaving].socketId === socket.id) {
                const userNameLeaving = connectedUsers[userIdLeaving].name;
                console.log(`User ${userNameLeaving} (${userIdLeaving}) left.`);

                delete connectedUsers[userIdLeaving];

                io.emit('userLeft', { userId: userIdLeaving }); 
                console.log("Total connected users:", Object.keys(connectedUsers).length);
            } else {

                 console.log(`Socket ${socket.id} disconnected, but user ${userIdLeaving} is now associated with socket ${connectedUsers[userIdLeaving].socketId}. Not removing user state.`);
            }
        } else {

            console.log(`Socket ${socket.id} disconnected, but no corresponding user found in connectedUsers (might have been removed already or never joined fully).`);
        }
    });

    socket.on('error', (err) => {
        console.error(`Socket error on ${socket.id} (User: ${socket.userId || 'N/A'}):`, err);

    });
});

function gracefulShutdown() {
    console.log('\nGraceful shutdown initiated...');

    clearInterval(saveIntervalId);

    clearTimeout(saveDebounceTimer);

    console.log('Performing final synchronous document save...');
    saveDocumentSync(); 

    console.log('Notifying clients of shutdown...');
    io.emit('serverShutdown', { message: 'Server is shutting down. Please save your work if possible.' });

    io.close(() => {
      console.log('Socket.IO connections closed.');

      server.close((err) => {
          if (err) {
              console.error("Error during HTTP server close:", err);
              process.exit(1); 
          }
          console.log('HTTP server closed. Exiting gracefully.');
          process.exit(0); 
      });
    });

    setTimeout(() => {
        console.error('Graceful shutdown timed out. Forcing exit.');
        process.exit(1); 
    }, 10000); 
}

process.on('SIGINT', gracefulShutdown);  
process.on('SIGTERM', gracefulShutdown); 
process.on('SIGUSR2', gracefulShutdown);
