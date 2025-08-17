console.log("gym.js loaded!");

// DataTables Instanz gespeichert, damit ich sie zerstören & neu initiieren kann
let tableInstance = null;

// Verhindert doppeltes Funktions-Laufen
const debounce = (fn, wait = 150) => {
  let t; return (...args) => { clearTimeout(t); t = setTimeout(() => fn(...args), wait); };
};


// Bindet alle Event-Listener, sobald DOM fertig geladen ist
document.addEventListener('DOMContentLoaded', () => {

  // 🔹 Modal-Elemente referenzieren
  const modal = document.getElementById("inputModal");
  const openBtn = document.getElementById("openModalBtn");
  const closeBtn = document.getElementById("closeModalBtn");

  // Person & Gym automatisch ins Modal setzen
  const headerPersonSelect = document.getElementById('person'); // im Header
  const modalPersonDisplay = document.getElementById('person-display'); // nur Text
  const modalPersonInput = document.getElementById('personModal'); // hidden input

  const headerGymSelect = document.getElementById('gym'); // Dropdown im Filter
  const modalGymDisplay = document.getElementById('gym-display'); // Anzeige im Modal
  const modalGymInput = document.getElementById('gymModal'); // Hidden Input

  
  function updateModalPerson() {
    if (headerPersonSelect && modalPersonDisplay && modalPersonInput) {
      modalPersonDisplay.textContent = headerPersonSelect.value;
      modalPersonInput.value = headerPersonSelect.value;
    }
  }
  // Falls im Header gewechselt wird → Modal-Daten auch updaten
  headerPersonSelect?.addEventListener('change', updateModalPerson);

  // Öffnen
  if (openBtn) {
    openBtn.addEventListener("click", () => {
      const headerPersonSelect = document.getElementById('person'); // Person-Dropdown  
      const modalPersonDisplay = document.getElementById('person-display');
      const modalPersonInput = document.getElementById('personModal');

      const headerGymSelect = document.getElementById('gym');       // Gym-Dropdown
      const modalGymInput = document.getElementById('gymModal');   // Eingabefeld im Modal
  
      const dateInput = document.getElementById('date');

      if (headerPersonSelect && modalPersonDisplay && modalPersonInput) {
        modalPersonDisplay.textContent = headerPersonSelect.value;
        modalPersonInput.value = headerPersonSelect.value;
      }

      if (headerGymSelect && modalGymInput) {
        modalGymInput.value = headerGymSelect.value; // Standardwert setzen
      }

      if (dateInput && !dateInput.value) {
        const today = new Date().toISOString().split('T')[0];
        dateInput.value = today;
      }

      modal.style.display = "block";
    });
  }

  // Schließen
  if (closeBtn) {
    closeBtn.addEventListener("click", () => {
      modal.style.display = "none";
    });
  }

  // Klick außerhalb schließt Modal
  window.addEventListener("click", (e) => {
    if (e.target === modal) {
      modal.style.display = "none";
    }
  });

  // 🔹 Form submit → POST an Flask
  const workoutForm = document.getElementById("workoutForm");
  if (workoutForm) {
    workoutForm.addEventListener("submit", async (e) => {
      e.preventDefault();
  
      const formData = {
        person: document.getElementById("personModal").value.trim(),
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
  
          // 🔹 Filterdaten anpassen, falls Datum außerhalb des Zeitraums liegt
          const startInput = document.getElementById("start");
          const endInput = document.getElementById("end");
          const startVal = startInput.value;
          const endVal = endInput.value;
  
          if (data.status === "success") {
            modal.style.display = "none";
            await fetchAndRender(); // keine direkte Manipulation von Start/End
          } else {
            alert("Fehler beim Speichern: " + (data.error || "unbekannt"));
          }

          // 🔹 Dashboard neu laden
          fetchAndRender();
        } else {
          alert("Fehler beim Speichern: " + (data.error || "unbekannt"));
        }
      } catch (err) {
        console.error("Fehler beim Senden:", err);
        alert("Verbindungsfehler beim Speichern");
      }
    });
  }

  const filterForm = document.querySelector('#filter-form');
  const metricSelect = document.getElementById('metric');
  const personSelect = document.getElementById('person'); // kann jetzt im Header liegen

  // Recalc bei Resize
  window.addEventListener('resize', debounce(() => {
    if (tableInstance) tableInstance.columns.adjust().responsive.recalc();
  }, 150));

  // Event-Listener für alle Filter im Formular (außer Person, falls außerhalb)
  if (filterForm) {
    filterForm.querySelectorAll('#gym, #start, #end').forEach(el =>
      el.addEventListener('change', async () => {
        await fetchAndRender();
      })
    );
  }

  // Person-Dropdown separat behandeln (unabhängig vom DOM-Ort)
  if (personSelect) {
    personSelect.addEventListener('change', async () => {
      document.getElementById("start").value = "";
      document.getElementById("end").value   = "";
      await fetchAndRender();
    });
  }
  
  const gymSelect = document.getElementById('gym');
  if (gymSelect) {
    gymSelect.addEventListener('change', async () => {
      document.getElementById("start").value = "";
      document.getElementById("end").value   = "";
      await fetchAndRender();
    });
  }

  // Metrik-Dropdown → nur Plot neu laden
  if (metricSelect) {
    metricSelect.addEventListener('change', fetchAndRenderPlot);
  }

  // Initial laden
  fetchAndRender();
  
});


function buildParams({includeMetric=true}={}){
  const form = document.querySelector('#filter-form');
  const metricSelect = document.getElementById('metric');
  const personSelect = document.getElementById('person');
  const fd = new FormData(form);
  const params = new URLSearchParams();

  for(const [k,v] of fd.entries()) params.append(k, v);

  if (personSelect && !params.has('person')) {
    params.append('person', personSelect.value);
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
