let tableInstance = null;
let originalFigData = [];


// Hilfs-Funktion für alle 3 fetchAndRender Funktionen
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


async function fetchAndRenderCards(){
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


async function fetchAndRenderTable(){
    const tableEl = document.getElementById('my-table');
    const tbodyEl = document.getElementById('table-body');
    const isMobile = window.innerWidth < 650;
  
    if ($.fn.DataTable.isDataTable(tableEl)) $(tableEl).DataTable().clear().destroy();
    tableInstance = null;
  
    try {
      const res = await fetch(`/api/gym-table?${buildParams()}`);
      if(!res.ok){
        tbodyEl.innerHTML = '<tr><td colspan="99">Fehler beim Laden der Tabelle</td></tr>';
      } else {
        const data = await res.json();
  
        tbodyEl.innerHTML = data.table_html;
  
        if (data.start) document.getElementById("start").value = data.start;
        if (data.end)   document.getElementById("end").value   = data.end;
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
            scrollY: isMobile ? '270px' : '305px',
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
    } 
    document.querySelectorAll('.ga-stat-value').forEach(el => {
        const value = el.textContent.trim();
        if (!/^\d+([.,]\d+)?$/.test(value)) {
        el.classList.add('long-text');
        }
      });
}


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

        originalFigData = fig.data;

        const metricDropdown = document.getElementById('metric');
        const selectedMetricText = metricDropdown?.options[metricDropdown.selectedIndex]?.text || '';

        const isMobile = window.innerWidth < 650;

        const layout = {
        showlegend: true,
        legend: isMobile
            ? {
                orientation: "h",
                yanchor: "top",
                y: -0.3,  
                xanchor: "center",
                x: 0.5,
                font: { size: 7 } 
            }
            : {
                orientation: "v", 
                x: 1,
                y: 1,
                font: { size: 11 } 
            },
        title: {
            text: `${selectedMetricText} pro Gerät im Zeitverlauf`,
            x: 0,
            xanchor: "left",
            pad: { t: 20, l: 10 },
            font: { size: isMobile ? 14 : 20 }
        },
        margin: { t: 50, l: 50, r: 20, b: isMobile ? 100 : 50 },
        height: isMobile ? 350 : 330, 
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


export function fetchAndRenderAll() {
    fetchAndRenderTable();
    fetchAndRenderCards();
    fetchAndRenderPlot();
}


export function initFilterRefresh() {
    const filterForm = document.querySelector('#filter-form');
    const metricSelect = document.getElementById('metric');
    const gymSelect = document.getElementById('gym');
  
    // Resize der Tabelle sauber steuern
    window.addEventListener('resize', (() => {
        let t;
        return () => {
        clearTimeout(t);
        t = setTimeout(() => {
            if (tableInstance) tableInstance.columns.adjust().responsive.recalc();
        }, 150);
        };
    })());
  
    // Wenn Start, Ende geändert wird: Tabelle refreshen
    if (filterForm) {
      filterForm.querySelectorAll('#start, #end').forEach(el =>
        el.addEventListener('change', fetchAndRenderTable)
      );
    }

    // Wenn Gym geändert wird: Alles refreshen
    if (gymSelect) {
      gymSelect.addEventListener('change', async () => {
        document.getElementById("start").value = "";
        document.getElementById("end").value   = "";
        await fetchAndRenderAllAll();
      });
    }
  
    // Wenn Plot Metrik geändert wird: Plot refreshen
    if (metricSelect) {
      metricSelect.addEventListener('change', fetchAndRenderPlot);
    }
}


export function initAnalysisToggle() {
  const btnTable = document.getElementById("show-table");
  const btnPlot = document.getElementById("show-plot");
  const sectionTable = document.getElementById("section-table");
  const sectionPlot = document.getElementById("section-plot");

  if (!btnTable || !btnPlot || !sectionTable || !sectionPlot) {
    console.warn("initAnalysisToggle: Elemente nicht gefunden.");
    return;
  }

  btnTable.addEventListener("click", (e) => {
    e.preventDefault();
    sectionTable.classList.remove("d-none");
    sectionPlot.classList.add("d-none");
    btnTable.classList.add("active");
    btnPlot.classList.remove("active");
  });

  // ⚡ wichtig: DataTable redraw triggern
  if ($.fn.dataTable.isDataTable('#my-table')) {
    $('#my-table').DataTable().columns.adjust().draw();
  }

  btnPlot.addEventListener("click", (e) => {
    e.preventDefault();
    sectionPlot.classList.remove("d-none");
    sectionTable.classList.add("d-none");
    btnPlot.classList.add("active");
    btnTable.classList.remove("active");

  // ⚡ wichtig: Plotly neu rendern
  if (document.getElementById("plot-container")) {
    Plotly.Plots.resize("plot-container");
  }


    const plotEl = document.getElementById("plot-container");
    if (plotEl) {
      Plotly.Plots.resize(plotEl);
    }
  });
}
