"use strict";

// Parse weather CSV file and returns array of arrays of data.
// The first line (headers) is ignored.
// The first returned array is a time series.
function parseWeatherCSV(csvLines) {
  const lines = csvLines.split(/\r?\n/); // Split by newline.
  const headers = lines[0].split(','); // Get headers from the first line.
  console.log("loaded %d lines with %d elements in each", lines.length, headers.length)
  const data = [];
  for (let j = 0; j < headers.length; j++) {
    data.push([]);
  }

  for (let i = 1; i < lines.length; i++) {
    if (lines[i].startsWith('#')) { // Skip comments
      continue;
    }
    const values = lines[i].split(',');
    if (values.length === headers.length) { // Ensure consistent data
      const row = {};
      for (let j = 0; j < headers.length; j++) {
        data[j].push(values[j].trim().replaceAll('_',' ')); // Trim whitespace
      }
    }
  }
  return data;
}

function fillCanvas(divName, xData, yData) {
  const sensorDiv = document.getElementById(divName);

  var data = [
    {
      x: xData,
      y: yData,
      type: 'scatter'
    }
  ];

  Plotly.newPlot(sensorDiv, data);
}

function loadAndDraw() {
  const queryString = window.location.search;
  const urlParams = new URLSearchParams(queryString);
  const name = urlParams.get('name');
  const days = urlParams.get('days');

  // Fetch sensor data.
  let url = '/data/' + name;
  console.log("url='" + url + "'; name='" + name + "'; days='" + days + "'");
  if (days !== null) {
    url = url + '?days=' + days;
  }
  console.log("url='" + url + "'");
  fetch(url)
    .then((response) => response.text())
    .then((content) => {
      let data = parseWeatherCSV(content);
      fillCanvas('temperature', data[0], data[1]);
      fillCanvas('pressure', data[0], data[2]);
      fillCanvas('humidity', data[0], data[3]);
    })
    .catch(function(error) {
      console.log(error);
    })
}
