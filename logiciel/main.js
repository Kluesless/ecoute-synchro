const { app, BrowserWindow, shell } = require('electron');

// ⚠️ Remplace par l'adresse que Render t'a donnée
// (pour tester en local : 'http://localhost:3000')
const SERVER_URL = 'https://TON-APPLI.onrender.com';

// Laisse la musique démarrer sans clic préalable
app.commandLine.appendSwitch('autoplay-policy', 'no-user-gesture-required');

function createWindow() {
  const win = new BrowserWindow({
    width: 900,
    height: 800,
    title: 'Écoute synchro',
    backgroundColor: '#14121f',
    autoHideMenuBar: true,
  });

  win.loadURL(SERVER_URL);

  // Les liens externes s'ouvrent dans le navigateur
  win.webContents.setWindowOpenHandler(({ url }) => {
    shell.openExternal(url);
    return { action: 'deny' };
  });

  // Si le serveur n'est pas joignable (ou se réveille), on réessaie
  win.webContents.on('did-fail-load', () => {
    setTimeout(() => win.loadURL(SERVER_URL), 3000);
  });
}

app.whenReady().then(createWindow);
app.on('window-all-closed', () => app.quit());
