console.log("gym.js loaded!");

// DataTables Instanz gespeichert, damit ich sie zerstören & neu initiieren kann
let tableInstance = null;

// Verhindert doppeltes Funktions-Laufen
const debounce = (fn, wait = 150) => {
  let t; return (...args) => { clearTimeout(t); t = setTimeout(() => fn(...args), wait); };
};


// Bindet alle Event-Listener, sobald DOM fertig geladen ist
document.addEventListener('DOMContentLoaded', () => {
  initWorkoutModal();        // Handling fürs Eingabe-Modal (inputModal)
  initWorkoutFormSubmit();   // Form submit → POST an Flask
  initFilters();             // Filter + Resize-Handling
  initPersonSelectModal();   // Start-Personenauswahl
});


function initPersonSelectModal() {
  const personStartModal = new bootstrap.Modal(document.getElementById('personModalStart'), {
    backdrop: 'static',
    keyboard: false
  });
  const confirmBtn = document.getElementById('confirm-person');
  const personSelectModal = document.getElementById('person-select-modal');

  personStartModal.show();

  confirmBtn.addEventListener('click', async () => {
    if (!personSelectModal.value) {
      alert("Bitte zuerst eine Person wählen.");
      return;
    }
    await fetchAndRender();
    personStartModal.hide();
  });
}


function initWorkoutModal() {
  const modal = document.getElementById("inputModal");
  const openBtn = document.getElementById("openModalBtn");
  const closeBtn = document.getElementById("closeModalBtn");

  if (openBtn) {
    openBtn.addEventListener("click", () => {
      const gymSelect = document.getElementById('gym');
      const modalGymInput = document.getElementById('gymModal');
      const dateInput = document.getElementById('date');

      if (gymSelect && modalGymInput) {
        modalGymInput.value = gymSelect.value;
      }
      if (dateInput && !dateInput.value) {
        dateInput.value = new Date().toISOString().split('T')[0];
      }

      modal.style.display = "block";
    });
  }

  if (closeBtn) {
    closeBtn.addEventListener("click", () => modal.style.display = "none");
  }

  window.addEventListener("click", (e) => {
    if (e.target === modal) modal.style.display = "none";
  });
}

function initWorkoutFormSubmit() {
  const modal = document.getElementById("inputModal");
  const workoutForm = document.getElementById("workoutForm");

  if (!workoutForm) return;

  workoutForm.addEventListener("submit", async (e) => {
    e.preventDefault();

    const formData = {
      person: document.getElementById("person-select-modal").value.trim(),
      gym: document.getElementById("gymModal").value.trim(),
      geraet: document.getElementById("geraet").value.trim(),
      saetze: parseInt(document.getElementById("saetze").value) || null,
      satz1_gew: parseFloat(document.getElementById("satz1_gew").value) || null,
      satz1_wdh: parseInt(document.getElementById("satz1_wdh").value) || null,
      satz2_gew: parseFloat(document.getElementById("satz2_gew").value) || null,
      satz2_wdh: parseInt(document.getElementById("satz2_wdh").value) || null,
      satz3_gew: parseFloat(document.getElementById("satz3_gew").value) || null,
      satz3_wdh: parseInt(document.getElementById("satz3_wdh").value) || null,
      datum: document.getElementById("date").value
    };

    try {
      const res = await fetch("/api/gym-insert", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(formData)
      });
      const data = await res.json();

      if (data.status === "success") {
        modal.style.display = "none";
        await fetchAndRender();
      } else {
        alert("Fehler beim Speichern: " + (data.error || "unbekannt"));
      }
    } catch (err) {
      console.error("Fehler beim Senden:", err);
      alert("Verbindungsfehler beim Speichern");
    }
  });
}

function initFilters() {
  const filterForm = document.querySelector('#filter-form');
  const metricSelect = document.getElementById('metric');
  const gymSelect = document.getElementById('gym');

  window.addEventListener('resize', debounce(() => {
    if (tableInstance) tableInstance.columns.adjust().responsive.recalc();
  }, 150));

  if (filterForm) {
    filterForm.querySelectorAll('#gym, #start, #end').forEach(el =>
      el.addEventListener('change', fetchAndRender)
    );
  }

  if (gymSelect) {
    gymSelect.addEventListener('change', async () => {
      document.getElementById("start").value = "";
      document.getElementById("end").value   = "";
      await fetchAndRender();
    });
  }

  if (metricSelect) {
    metricSelect.addEventListener('change', fetchAndRenderPlot);
  }
}


function buildParams({includeMetric=true}={}){
  const form = document.querySelector('#filter-form');
  const metricSelect = document.getElementById('metric');
  const personSelectModal = document.getElementById('person-select-modal');
  const fd = new FormData(form);
  const params = new URLSearchParams();

  for(const [k,v] of fd.entries()) params.append(k, v);

  if (personSelectModal && !params.has('person')) {
    params.append('person', personSelectModal.value);
  }

  if(includeMetric && metricSelect && !params.has('metric')) {
    params.append('metric', metricSelect.value);
  }
  
  params.append('_ts', Date.now()); // cache bust
  return params.toString();
}

async function fetchAndRender(){
  const tableEl = document.getElementById('my-table');
  const tbodyEl = document.getElementById('table-body');

  // DataTable sauber zerstören, bevor wir DOM tauschen
  if ($.fn.DataTable.isDataTable(tableEl)) $(tableEl).DataTable().clear().destroy();
  tableInstance = null;

  try {
    const res = await fetch(`/api/gym-table?${buildParams()}`);
    if(!res.ok){
      tbodyEl.innerHTML = '<tr><td colspan="99">Fehler beim Laden der Tabelle</td></tr>';
    } else {
      const data = await res.json();

      // Tabelle einsetzen
      tbodyEl.innerHTML = data.table_html;

      // Start-/Enddatum nur setzen, wenn Werte vorhanden sind
      if (data.min_date) document.getElementById("start").value = data.min_date;
      if (data.max_date) document.getElementById("end").value = data.max_date;

      const hasRows = !!tbodyEl.querySelector('tr');
      const headerCount = document.querySelectorAll('#my-table thead th').length;
      const validRowExists = [...tbodyEl.querySelectorAll('tr')]
        .some(tr => tr.querySelectorAll('td').length === headerCount);

      if (hasRows && validRowExists) {
        tableInstance = $('#my-table').DataTable({
          paging:false,  
          info:false, 
          searching:false, 
          responsive:true, 
          destroy:true,
          scrollY:'208px', 
          dom:'rt<"dt-bottom"l p>',
          order: [[1, 'desc']],
          columnDefs: [
            {
              targets: 2,
              render: function (data, type) {
                if (type === 'display' && data != null) {
                  return parseInt(data).toLocaleString('de-DE');
                }
                return data;
              }
            },
            {
              targets: [3, 4],
              render: function (data, type) {
                if (type === 'display' && data != null) {
                  return parseFloat(data).toLocaleString('de-DE', {
                    minimumFractionDigits: 1,
                    maximumFractionDigits: 1
                  });
                }
                return data;
              }
            }
          ]
        });
      } else {
        console.warn('DataTables nicht initialisiert – unpassende Struktur');
        document.getElementById('plot-container').innerHTML = '<p>Keine Geräte für diesen Zeitraum.</p>';
      }
    }
  } catch(err) {
    console.error("[table] Fehler:", err);
    tbodyEl.innerHTML = '<tr><td colspan="99">Fehler beim Laden der Tabelle</td></tr>';
  } finally {
    // 🔹 wird **immer** ausgeführt, egal ob Fehler, leer oder Erfolg
    await fetchAndRenderPlot();
    await fetchAndRenderOverview();
  }

  markLongTextValues();
}

async function fetchAndRenderOverview(){
  const container = document.getElementById('overview');
  if (!container) return;

  try{
    const res = await fetch(`/api/gym-overview?${buildParams({includeMetric:false})}`);
    if(!res.ok){
      const t = await res.text().catch(()=> '');
      console.error('[overview] HTTP error:', res.status, t);
      container.innerHTML = '<div class="col-12"><p style="color:#f88;">Fehler beim Laden der Übersicht.</p></div>';
      return;
    }
    const html = await res.text();
    container.innerHTML = html || '<div class="col-12"><p>Keine Daten.</p></div>';
  }catch(e){
    console.error('[overview] JS error:', e);
    container.innerHTML = '<div class="col-12"><p style="color:#f88;">Fehler beim Verarbeiten der Übersicht.</p></div>';
  }
}

function markLongTextValues() {
  document.querySelectorAll('.ga-stat-value').forEach(el => {
    const value = el.textContent.trim();
    if (!/^\d+([.,]\d+)?$/.test(value)) {
      el.classList.add('long-text');
    }
  });
}




let originalFigData = [];

async function fetchAndRenderPlot() {
  const container = document.getElementById('plot-container');

  try {
    const res = await fetch(`/api/gym-plot?${buildParams({ includeMetric: true })}`);
    if (!res.ok) {
      container.innerHTML = '<p style="color:#f88;">Fehler beim Laden des Diagramms.</p>';
      return;
    }

    const fig = await res.json();

    if (!Array.isArray(fig?.data) || fig.data.length === 0) {
      container.innerHTML = '<p>Kein Diagramm für diese Auswahl.</p>';
      return;
    }

    // Originaldaten sichern
    originalFigData = fig.data;

    // Plot mit Legende anzeigen
    const metricDropdown = document.getElementById('metric');
    const selectedMetricText = metricDropdown?.options[metricDropdown.selectedIndex]?.text || '';

    const isMobile = window.innerWidth < 650;

    // Layout in Plot nicht mit css steuerbar!
    const layout = {
      showlegend: true,
      legend: isMobile
        ? {
            orientation: "h",    // horizontal
            yanchor: "top",
            y: -0.3,              // unterhalb der X-Achse
            xanchor: "center",
            x: 0.5,
            font: { size: 8 }    // kleinere Schrift
          }
        : {
            orientation: "v",    // Desktop bleibt vertikal rechts
            x: 1,
            y: 1,
            font: { size: 11 } // Browser-Schriftgröße (1 kleiner als vorher)
          },
      title: {
        text: `${selectedMetricText} pro Gerät im Zeitverlauf`,
        x: 0,
        xanchor: "left",
        pad: { t: 20, l: 10 },
        font: { size: isMobile ? 14 : 20 }
      },
      margin: { t: 50, l: 50, r: 20, b: isMobile ? 100 : 50 } // unten mehr Platz bei Handy
    };

    const config = {
      responsive: true,
      displaylogo: false,
      modeBarButtonsToRemove: [
        'zoom2d', 'pan2d', 'select2d', 'lasso2d',
        'zoomIn2d', 'zoomOut2d', 'autoScale2d', 
        'hoverClosestCartesian', 'hoverCompareCartesian',
        'toggleSpikelines', 'resetViewMapbox'
      ],
    };

    Plotly.newPlot(container, originalFigData, layout,  config);

  } catch (err) {
    console.error('[plot] Fehler:', err);
    container.innerHTML = '<p style="color:#f88;">Fehler beim Verarbeiten der Plot-Daten.</p>';
  }
}
