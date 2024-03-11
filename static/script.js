// This function is intended for loading data based on a search key,
// which is set by the server-rendered page if a search was performed.
async function loadData(searchKey) {
    const response = await fetch(`/api/results/${searchKey}`);
    const data = await response.json();
    displayResults(data.result); // Assuming the API wraps results in a 'result' object
}

// This event listener is for handling form submissions, which trigger a new search.
// It constructs a URL with query parameters and uses history.pushState for SPA behavior.
document.getElementById('searchForm').addEventListener('submit', async function(event) {
    event.preventDefault();
    const searchStr = document.getElementById('searchStr').value;
    const newUrl = `${window.location.pathname}?search_str=${encodeURIComponent(searchStr)}`;
    window.history.pushState({ path: newUrl }, '', newUrl);
    
    // Redirecting to the root with search parameters instead of making a fetch request here
    // This is because your server-side logic expects to handle new searches via the root endpoint.
    window.location.href = newUrl;
});

// This function is responsible for displaying the results. It dynamically creates a table
// based on the data fetched from either the initial load or a new search.
function displayResults(data) {
    const resultsContainer = document.getElementById('results');
    resultsContainer.innerHTML = ''; // Clear previous results

    if (data && data.length > 0) {
        let resultsText = document.createElement('p');
        resultsText.innerHTML = `${data.length} mapping relationships found.`;
        resultsContainer.appendChild(resultsText);

        const table = document.createElement('table');
        table.setAttribute('border', '1');

        // Create headers using the `columnNames` variable
        const headerRow = document.createElement('tr');
        columnNames.forEach(colName => {
            const headerCell = document.createElement('th');
            headerCell.textContent = colName;
            headerRow.appendChild(headerCell);
        });
        table.appendChild(headerRow);

        // Add rows (ensure the data structure aligns with your column names)
        data.forEach(rowData => {
            const row = document.createElement('tr');
            columnNames.forEach(colName => {
                const cell = document.createElement('td');
                cell.textContent = rowData[colName]; // Adjust based on your data structure
                row.appendChild(cell);
            });
            table.appendChild(row);
        });

        resultsContainer.appendChild(table);
    } else {
        resultsContainer.innerHTML = '<p>No results found.</p>';
    }
}

// Use the `DOMContentLoaded` event to check if a searchKey is available for initial load
document.addEventListener("DOMContentLoaded", function() {
    // Assuming `searchKey` is globally defined in your HTML by server-side templating
    if (typeof searchKey !== 'undefined' && searchKey) {
        loadData(searchKey);
    }
});
