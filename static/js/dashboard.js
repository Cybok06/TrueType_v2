function initDashboard() {
  fetch('/home/details')
    .then(async response => {
      if (!response.ok) {
        const errData = await response.json().catch(() => ({}));
        throw new Error(errData?.error || 'Failed to fetch dashboard data.');
      }
      return response.json();
    })
    .then(data => {
      const chartSection = document.getElementById('chart-section');
      const activitySection = document.getElementById('activity-section');
      const percentage = data.total_debt > 0
        ? Math.round((data.total_paid / data.total_debt) * 100)
        : 0;

      chartSection.innerHTML = `
        <div class="chart-card mb-4">
          <h6 class="text-center">📊 Monthly Orders Overview</h6>
          <div class="chart-container"><canvas id="ordersChart"></canvas></div>
        </div>
        <div class="chart-card mb-4">
          <h6 class="text-center">🏆 Top Clients by Orders</h6>
          <div class="chart-container"><canvas id="topClientsChart"></canvas></div>
        </div>
        <div class="chart-card mb-4 text-center">
          <h6 class="mb-3">💰 Collection Rate</h6>
          <div class="chart-container d-flex justify-content-center">
            <canvas id="debtChart" style="max-width: 250px;"></canvas>
          </div>
          <p class="mt-3">Out of GHS ${data.total_debt}, you've collected GHS ${data.total_paid}</p>
        </div>
      `;

      requestAnimationFrame(() => renderCharts(data, percentage));

      // === RECENT ACTIVITIES ===
      if (data.recent_activities && data.recent_activities.length > 0) {
        activitySection.innerHTML = `
          <div class="chart-card shadow-sm rounded p-3">
            <h6 class="mb-3 fw-bold">🕒 Recent Activities</h6>
            <ul class="list-group list-group-flush" id="activityList"></ul>
          </div>
        `;
        const list = document.getElementById('activityList');
        data.recent_activities.forEach(act => {
          const li = document.createElement("li");
          li.className = "list-group-item d-flex justify-content-between align-items-start flex-wrap px-0 py-2";
          li.innerHTML = `
            <div class="d-flex align-items-start">
              <span class="fs-5 me-2 ${act.color}">${act.icon}</span>
              <div><div class="fw-medium ${act.color}">${act.text}</div></div>
            </div>
            <small class="text-muted ms-auto mt-1">${new Date(act.time).toLocaleString()}</small>
          `;
          list.appendChild(li);
        });
      } else {
        activitySection.innerHTML = `<p class="text-muted text-center">No recent activities</p>`;
      }
    })
    .catch(error => {
      console.error("Dashboard Error:", error);
      document.getElementById('chart-section').innerHTML = `<p class="text-danger">Chart load failed: ${error.message}</p>`;
      document.getElementById('activity-section').innerHTML = `<p class="text-danger">Activity load failed: ${error.message}</p>`;
    });

  function renderCharts(data, percentage) {
    new Chart(document.getElementById('ordersChart'), {
      type: 'bar',
      data: {
        labels: data.months,
        datasets: [{
          label: 'Orders',
          data: data.order_counts,
          backgroundColor: '#007bff'
        }]
      },
      options: {
        responsive: true,
        scales: { y: { beginAtZero: true } }
      }
    });

    new Chart(document.getElementById('topClientsChart'), {
      type: 'bar',
      data: {
        labels: data.top_clients_names,
        datasets: [{
          label: 'Orders',
          data: data.top_clients_orders,
          backgroundColor: '#ffc107'
        }]
      },
      options: {
        responsive: true,
        indexAxis: 'y',
        scales: { x: { beginAtZero: true } }
      }
    });

    new Chart(document.getElementById('debtChart'), {
      type: 'doughnut',
      data: {
        labels: ['Collected', 'Remaining'],
        datasets: [{
          data: [data.total_paid, data.total_debt - data.total_paid],
          backgroundColor: ['#28a745', '#dc3545'],
          borderWidth: 1
        }]
      },
      options: {
        responsive: true,
        cutout: '70%',
        plugins: { legend: { display: false } }
      },
      plugins: [{
        id: 'centerText',
        beforeDraw(chart) {
          const { width } = chart;
          const text = `${percentage}%`;
          chart.ctx.restore();
          chart.ctx.font = 'bold 22px Segoe UI';
          chart.ctx.textBaseline = 'middle';
          chart.ctx.textAlign = 'center';
          chart.ctx.fillText(text, width / 2, 105);
          chart.ctx.save();
        }
      }]
    });
  }
}
