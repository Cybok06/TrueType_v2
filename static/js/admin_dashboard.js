function renderDebtorCharts() {
  const chartScript = document.getElementById('client-data');
  if (!chartScript) return;

  const chartData = JSON.parse(chartScript.textContent || '[]');

  chartData.forEach((client, index) => {
    const compareCanvas = document.getElementById(`compare-chart-${index + 1}`);
    const timelineCanvas = document.getElementById(`timeline-chart-${index + 1}`);

    // Bar Chart
    if (compareCanvas && client.total_debt > 0) {
      new Chart(compareCanvas.getContext('2d'), {
        type: 'bar',
        data: {
          labels: ['Total Paid', 'Amount Left'],
          datasets: [{
            label: 'GHS',
            data: [client.total_paid, client.total_debt - client.total_paid],
            backgroundColor: ['#28a745', '#dc3545']
          }]
        },
        options: {
          responsive: true,
          plugins: {
            title: { display: true, text: 'Total Paid vs Amount Left' },
            legend: { display: false }
          },
          scales: {
            y: {
              beginAtZero: true,
              title: { display: true, text: 'Amount (GHS)' }
            }
          }
        }
      });
    }

    // Line Chart
    if (timelineCanvas && client.payments.length > 0) {
      const labels = client.payments.map(p => p.date);
      const data = client.payments.map(p => p.amount);

      new Chart(timelineCanvas.getContext('2d'), {
        type: 'line',
        data: {
          labels,
          datasets: [{
            label: 'Confirmed Payments',
            data,
            borderColor: '#007bff',
            backgroundColor: 'rgba(0,123,255,0.1)',
            fill: true,
            tension: 0.3,
            pointRadius: 4,
            pointHoverRadius: 6
          }]
        },
        options: {
          responsive: true,
          plugins: {
            legend: { display: true },
            title: { display: false }
          },
          scales: {
            y: {
              beginAtZero: true,
              title: { display: true, text: 'Amount (GHS)' }
            },
            x: {
              title: { display: true, text: 'Date' }
            }
          }
        }
      });
    }
  });
}

function loadContent(url, label = '') {
  $('#content-area').html(`
    <div class="d-flex align-items-center text-muted">
      <div class="spinner-border text-primary me-2" role="status" aria-hidden="true"></div>
      Loading...
    </div>
  `);

  $.get(url)
    .done(function (data) {
      $('#content-area').html(data);

      if (label) {
        $('#section-title').text(label);
        localStorage.setItem('adminLastUrl', url);
        localStorage.setItem('adminLastLabel', label);
      }

      if (url === '/home') {
        $.getScript('/static/js/dashboard.js', function () {
          // Call manually after dashboard.js is loaded
          if (typeof initDashboard === 'function') {
            initDashboard();
          }
        });
      }

      if (url === '/debtors') {
        setTimeout(renderDebtorCharts, 150);
      }
    })
    .fail(function () {
      $('#content-area').html('<div class="alert alert-danger">Failed to load content.</div>');
    });
}

$(document).ready(function () {
  const lastUrl = localStorage.getItem('adminLastUrl') || '/home';
  const lastLabel = localStorage.getItem('adminLastLabel') || '';

  loadContent(lastUrl, lastLabel);

  $('.sidebar-link').each(function () {
    if ($(this).data('url') === lastUrl) {
      $(this).addClass('active-link');
    }
  });
});

$(document).on('click', '.sidebar-link[data-url]', function (e) {
  e.preventDefault();
  const url = $(this).data('url');
  const label = $(this).text().trim();
  $('.sidebar-link').removeClass('active-link');
  $(this).addClass('active-link');
  loadContent(url, label);
});
