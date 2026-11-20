/**
 * Lumina Retail - Plataforma de Inteligencia Comercial
 * Frontend Logic & Conversational Agent Simulation
 * Grupo 7 - UTEC Big Data Architecture
 */

// Configuración base de la API
const urlParams = new URLSearchParams(window.location.search);
const API_BASE_URL = urlParams.get('api') || window.location.origin;

let chartSourcesInstance = null;
let chartUnitsInstance = null;

// Tab Switching
function switchTab(tabId) {
  document.querySelectorAll('.tab-button').forEach(btn => btn.classList.remove('active'));
  document.querySelectorAll('.tab-content').forEach(content => content.classList.remove('active'));

  event.currentTarget.classList.add('active');
  const target = document.getElementById(`tab-${tabId}`);
  if (target) target.classList.add('active');

  if (tabId === 'dashboard') {
    loadDashboardData();
  }
}

// Inicialización
window.addEventListener('DOMContentLoaded', () => {
  checkHealth();
  loadDashboardData();
});

// Chequeo de Salud del Backend
async function checkHealth() {
  const statusLabel = document.getElementById('api-status-label');
  const statusDot = document.querySelector('.status-dot');

  try {
    const res = await fetch(`${API_BASE_URL}/health`);
    if (res.ok) {
      const data = await res.json();
      statusLabel.textContent = `BigQuery Gold Conectado (${data.status.toUpperCase()})`;
      statusDot.style.background = '#10b981';
      statusDot.style.boxShadow = '0 0 8px #10b981';
    } else {
      throw new Error('Respuesta no exitosa');
    }
  } catch (err) {
    statusLabel.textContent = 'API Desconectada (Verifica puerto 8080)';
    statusDot.style.background = '#ef4444';
    statusDot.style.boxShadow = '0 0 8px #ef4444';
  }
}

// Formateador de Moneda USD
function formatUSD(amount) {
  return new Intl.NumberFormat('en-US', {
    style: 'currency',
    currency: 'USD',
    maximumFractionDigits: 2
  }).format(amount || 0);
}

// Formateador de Enteros
function formatNumber(num) {
  return new Intl.NumberFormat('en-US').format(num || 0);
}

// Carga de Datos del Dashboard
async function loadDashboardData() {
  const yearVal = document.getElementById('filter-year').value;
  const sourceVal = document.getElementById('filter-source').value;

  const queryParams = new URLSearchParams();
  if (yearVal) queryParams.append('year', yearVal);
  if (sourceVal) queryParams.append('source_system', sourceVal);

  try {
    // 1. Cargar KPIs Principales (Matriz Ejecutiva 3x2)
    const kpiRes = await fetch(`${API_BASE_URL}/kpis/summary?${queryParams.toString()}`);
    if (kpiRes.ok) {
      const kpis = await kpiRes.json();
      document.getElementById('kpi-revenue').textContent = formatUSD(kpis.net_revenue_usd);
      document.getElementById('kpi-gross-sub').textContent = `Ingreso Bruto: ${formatUSD(kpis.gross_revenue_usd)}`;
      document.getElementById('kpi-orders').textContent = formatNumber(kpis.total_orders);
      document.getElementById('kpi-units-sub').textContent = `Unidades vendidas: ${formatNumber(kpis.total_units_sold)}`;
      document.getElementById('kpi-ticket').textContent = formatUSD(kpis.average_ticket_usd);
      document.getElementById('kpi-margin').textContent = formatUSD(kpis.total_estimated_margin_usd);
      document.getElementById('kpi-margin-pct').textContent = `Margen operativo: ${kpis.estimated_margin_pct}%`;
      
      // Nuevas Tarjetas 5 y 6
      const elOntime = document.getElementById('kpi-ontime');
      if (elOntime) elOntime.textContent = `${kpis.on_time_delivery_rate_pct}%`;
      const elOntimeSub = document.getElementById('kpi-ontime-sub');
      if (elOntimeSub) elOntimeSub.textContent = `Cumplimiento de entrega (${yearVal || 'Histórico'})`;

      const elCancel = document.getElementById('kpi-cancellations');
      if (elCancel) elCancel.textContent = `${kpis.cancellation_return_rate_pct}%`;
      const elCancelSub = document.getElementById('kpi-cancellations-sub');
      if (elCancelSub) elCancelSub.textContent = `Devoluciones sobre líneas de orden`;
    }

    // 1.1 Cargar Métricas Logísticas en barra inferior
    try {
      const logRes = await fetch(`${API_BASE_URL}/logistics/summary?${queryParams.toString()}`);
      if (logRes.ok) {
        const logData = await logRes.json();
        const elFreight = document.getElementById('op-freight-avg');
        if (elFreight) elFreight.textContent = `${formatUSD(logData.average_freight_cost_usd)} / orden`;
        const elHours = document.getElementById('op-delivery-hours');
        if (elHours) elHours.textContent = `${logData.average_delivery_hours} hrs (~${(logData.average_delivery_hours / 24).toFixed(1)} días)`;
      }
    } catch (e) {
      console.warn('Endpoint logístico no disponible temporalmente:', e);
    }

    // 2. Cargar Distribución por Fuente
    const sourceParams = new URLSearchParams();
    if (yearVal) sourceParams.append('year', yearVal);
    const sourceRes = await fetch(`${API_BASE_URL}/sales/by-source?${sourceParams.toString()}`);
    if (sourceRes.ok) {
      const sourceData = await sourceRes.json();
      renderSourcesChart(sourceData.sources);
    }

    // 3. Cargar Top Unidades de Negocio
    const unitRes = await fetch(`${API_BASE_URL}/sales/by-unit?${queryParams.toString()}`);
    if (unitRes.ok) {
      const unitData = await unitRes.json();
      renderUnitsChart(unitData.units.slice(0, 6)); // Top 6
    }
  } catch (err) {
    console.error('Error cargando datos del dashboard:', err);
  }
}

// Renderizar Gráfico de Fuentes (Doughnut)
function renderSourcesChart(sources) {
  const ctx = document.getElementById('chart-sources').getContext('2d');
  const labels = sources.map(s => `${s.source_system} (${s.share_revenue_pct}%)`);
  const values = sources.map(s => s.net_revenue_usd);

  if (chartSourcesInstance) {
    chartSourcesInstance.destroy();
  }

  chartSourcesInstance = new Chart(ctx, {
    type: 'doughnut',
    data: {
      labels: labels,
      datasets: [{
        data: values,
        backgroundColor: ['#1e3a8a', '#0284c7', '#d97706', '#475569'],
        borderColor: '#ffffff',
        borderWidth: 3,
        hoverOffset: 6
      }]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: {
        legend: {
          position: 'right',
          labels: {
            color: '#1e293b',
            font: { family: 'Inter', size: 12, weight: '500' },
            padding: 14,
            usePointStyle: true,
            pointStyle: 'circle'
          }
        },
        tooltip: {
          backgroundColor: '#0f172a',
          titleColor: '#ffffff',
          bodyColor: '#e2e8f0',
          padding: 10,
          cornerRadius: 6,
          callbacks: {
            label: function(context) {
              const val = context.raw || 0;
              return ` Facturación: ${formatUSD(val)}`;
            }
          }
        }
      }
    }
  });
}

// Renderizar Gráfico de Unidades (Barra Horizontal)
function renderUnitsChart(units) {
  const ctx = document.getElementById('chart-units').getContext('2d');
  const labels = units.map(u => u.business_unit_name.length > 25 ? u.business_unit_name.substring(0, 22) + '...' : u.business_unit_name);
  const values = units.map(u => u.net_revenue_usd);

  if (chartUnitsInstance) {
    chartUnitsInstance.destroy();
  }

  chartUnitsInstance = new Chart(ctx, {
    type: 'bar',
    data: {
      labels: labels,
      datasets: [{
        label: 'Ingresos Netos (USD)',
        data: values,
        backgroundColor: '#1e40af',
        hoverBackgroundColor: '#1d4ed8',
        borderRadius: 5,
        maxBarThickness: 26
      }]
    },
    options: {
      indexAxis: 'y',
      responsive: true,
      maintainAspectRatio: false,
      scales: {
        x: {
          grid: { color: '#f1f5f9', drawBorder: false },
          ticks: {
            color: '#64748b',
            font: { family: 'Inter', size: 11 },
            callback: value => `$${(value / 1000).toFixed(0)}K`
          }
        },
        y: {
          grid: { display: false },
          ticks: {
            color: '#1e293b',
            font: { family: 'Inter', size: 11, weight: '500' }
          }
        }
      },
      plugins: {
        legend: { display: false },
        tooltip: {
          backgroundColor: '#0f172a',
          titleColor: '#ffffff',
          bodyColor: '#e2e8f0',
          padding: 10,
          cornerRadius: 6,
          callbacks: {
            label: context => ` Ingresos Netos: ${formatUSD(context.raw)}`
          }
        }
      }
    }
  });
}

// LÓGICA DEL AGENTE CONVERSACIONAL (Lumina Business Assistant)
async function handleChatSubmit(e) {
  e.preventDefault();
  const inputEl = document.getElementById('chat-input');
  const query = inputEl.value.trim();
  if (!query) return;

  inputEl.value = '';
  appendUserMessage(query);
  await processAgentQuery(query);
}

function askPreset(promptText) {
  switchTab('agent');
  appendUserMessage(promptText);
  processAgentQuery(promptText);
}

function appendUserMessage(text) {
  const messagesBox = document.getElementById('chat-messages');
  const msgDiv = document.createElement('div');
  msgDiv.className = 'chat-message user';
  msgDiv.innerHTML = `
    <div class="message-avatar">👤</div>
    <div class="message-body"><p>${text}</p></div>
  `;
  messagesBox.appendChild(msgDiv);
  messagesBox.scrollTop = messagesBox.scrollHeight;
}

function appendBotMessage(htmlContent) {
  const messagesBox = document.getElementById('chat-messages');
  const msgDiv = document.createElement('div');
  msgDiv.className = 'chat-message bot';
  msgDiv.innerHTML = `
    <div class="message-avatar">L</div>
    <div class="message-body">${htmlContent}</div>
  `;
  messagesBox.appendChild(msgDiv);
  messagesBox.scrollTop = messagesBox.scrollHeight;
}

// SESIÓN CONVERSACIONAL Y PROCESAMIENTO EN GOOGLE CLOUD CX AGENT STUDIO
let currentChatSessionId = 'web-session-' + Date.now() + '-' + Math.random().toString(36).substring(2, 7);

function markdownToHtml(md) {
  if (!md) return '';
  
  let text = md.replace(/\r\n/g, '\n');
  text = text.replace(/^### (.*$)/gim, '<h4 style="margin: 10px 0 6px; color: #1e293b;">$1</h4>');
  text = text.replace(/^## (.*$)/gim, '<h3 style="margin: 12px 0 8px; color: #0f172a;">$1</h3>');
  text = text.replace(/\*\*(.*?)\*\*/g, '<strong>$1</strong>');
  text = text.replace(/\*(.*?)\*/g, '<em>$1</em>');

  const lines = text.split('\n');
  let inList = false;
  let formattedLines = [];

  for (let line of lines) {
    const listMatch = line.match(/^\s*[\*\-]\s+(.*)$/);
    if (listMatch) {
      if (!inList) {
        formattedLines.push('<ul style="margin: 8px 0 10px 18px; padding-left: 0; line-height: 1.6;">');
        inList = true;
      }
      formattedLines.push('<li>' + listMatch[1] + '</li>');
    } else {
      if (inList) {
        formattedLines.push('</ul>');
        inList = false;
      }
      if (line.trim().length > 0) {
        formattedLines.push('<p style="margin: 6px 0;">' + line.trim() + '</p>');
      }
    }
  }
  if (inList) {
    formattedLines.push('</ul>');
  }

  return formattedLines.join('');
}

async function processAgentQuery(query) {
  const typingId = 'typing-' + Date.now();
  appendBotMessage(
    '<div id="' + typingId + '" style="display: flex; align-items: center; gap: 8px; color: #64748b; font-size: 13px; font-style: italic;">' +
      '<span style="display:inline-block; width:12px; height:12px; border:2px solid #2563eb; border-top-color:transparent; border-radius:50%; animation: spin 0.8s linear infinite;"></span>' +
      '<span>Lulita está consultando Google Cloud CX Agent Studio & BigQuery Gold...</span>' +
    '</div>'
  );

  try {
    const res = await fetch(API_BASE_URL + '/agent/chat', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        message: query,
        session_id: currentChatSessionId
      })
    });

    const typingEl = document.getElementById(typingId);
    if (typingEl && typingEl.closest('.chat-message')) {
      typingEl.closest('.chat-message').remove();
    }

    if (!res.ok) {
      const errData = await res.json().catch(() => ({}));
      throw new Error(errData.detail || ('Error HTTP ' + res.status));
    }

    const data = await res.json();
    const formattedHtml = markdownToHtml(data.reply);

    appendBotMessage(
      '<div class="tool-badge" style="color: #1e40af; border-color: #bfdbfe; font-size: 11px; margin-bottom: 6px;">' +
        '🤖 Google Cloud CX Agent Studio · ' + (data.match_type || 'PLAYBOOK') + ' · Gemini' +
      '</div>' +
      formattedHtml
    );
  } catch (err) {
    const typingEl = document.getElementById(typingId);
    if (typingEl && typingEl.closest('.chat-message')) {
      typingEl.closest('.chat-message').remove();
    }
    appendBotMessage(
      '<div class="tool-badge" style="color: #dc2626; border-color: #fca5a5; font-size: 11px; margin-bottom: 6px;">' +
        '⚠️ Error de Conexión' +
      '</div>' +
      '<p>Ocurrió un error al consultar el motor de Google Cloud CX Agent Studio: <strong>' + err.message + '</strong></p>' +
      '<p><em>Verifica que el servicio en Google Cloud Run y la API de Dialogflow CX se encuentren activos.</em></p>'
    );
  }
}
