/* 
  This is a simple Express server that serves static files from the 'public' directory.
  It listens on port 3000 by default, or the port defined in the environment variable 'PORT'.
*/

const express = require('express');
const path = require('path');

const app = express();
const PORT = process.env.PORT || 3000;

// Middleware: Serve static files from the 'public' directory
app.use(express.static(path.join(__dirname, 'public')));

// (Optional) Define a route for the root URL to serve index.html
app.get('/', (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

// Start the server
app.listen(PORT, () => {
  console.log(`Server is running on http://localhost:${PORT}`);
});
