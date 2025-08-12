console.log("gym.js loaded!");

// DataTables Instanz gespeichert, damit ich sie zerstören & neu initiieren kann
let tableInstance = null;

// Menge aller aktuell markierten Geräte
let selectedGeraete = new Set(); 

// Verhindert doppeltes Funktions-Laufen
const debounce = (fn, wait = 150) => {
  let t; return (...args) => { clearTimeout(t); t = setTimeout(() => fn(...args), wait); };
};


// Bindet alle Event-Listener, sobald DOM fertig geladen ist
document.addEventListener('DOMContentLoaded', () => {

  const form = document.querySelector('#filter-form');
  const metricSelect = document.getElementById('metric');
  const personSelect = document.getElementById('person'); // kann jetzt im Header liegen

  // Recalc bei Resize
  window.addEventListener('resize', debounce(() => {
    if (tableInstance) tableInstance.columns.adjust().responsive.recalc();
  }, 150));

  // Event-Listener für alle Filter im Formular (außer Person, falls außerhalb)
  if (form) {
    form.querySelectorAll('#gym, #start, #end').forEach(el =>
      el.addEventListener('change', async () => {
        selectedGeraete.clear();
        await fetchAndRender();
      })
    );
  }

  // Person-Dropdown separat behandeln (unabhängig vom DOM-Ort)
  if (personSelect) {
    personSelect.addEventListener('change', async () => {
      selectedGeraete.clear();
      await fetchAndRender();
    });
  }

  // Metrik-Dropdown → nur Plot neu laden
  if (metricSelect) {
    metricSelect.addEventListener('change', fetchAndRenderPlot);
  }

  // Buttons: Alle auswählen / abwählen
  $(document)
    .off('click', '#select-all')
    .on('click', '#select-all', e => {
      e.preventDefault();
      selectedGeraete = new Set(getAllGeraeteFromTable());
      highlightSelection();
      fetchAndRenderPlot();
      markLongTextValues();
    })
    .off('click', '#deselect-all')
    .on('click', '#deselect-all', e => {
      e.preventDefault();
      selectedGeraete.clear();
      highlightSelection();
      fetchAndRenderPlot();
      markLongTextValues();
    });

  // Initial laden
  fetchAndRender();
  
  document.querySelectorAll('#my-table thead .th-short[data-bs-toggle="tooltip"]')
  .forEach(el => new bootstrap.Tooltip(el));
});



// Hilfs-Funktion
function getAllGeraeteFromTable(){
  const out=[]; document.querySelectorAll('#my-table tbody tr').forEach(r=>{
    if(r.dataset.geraet) out.push(r.dataset.geraet);
  }); return out;
}

// Hilfs-Funktion
function highlightSelection(){
  document.querySelectorAll('#my-table tbody tr').forEach(row=>{
    row.classList.toggle('table-active', selectedGeraete.has(row.dataset.geraet));
  });
}


// Baut den Querystring für die API Request
function buildParams({includeMetric=true, includeGeraete=false}={}){
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
  
  if(includeGeraete && selectedGeraete.size>0) {
    selectedGeraete.forEach(g => params.append('geraete', g));
  }
  
  params.append('_ts', Date.now()); // cache bust
  return params.toString();
}


// Holt Tabellendaten aus api/gym-table
async function fetchAndRender(){
  const tableEl = document.getElementById('my-table');
  const tbodyEl = document.getElementById('table-body');

  // DataTable sauber zerstören, bevor wir DOM tauschen
  if ($.fn.DataTable.isDataTable(tableEl)) $(tableEl).DataTable().clear().destroy();
  tableInstance = null;

  const res = await fetch(`/api/gym-table?${buildParams()}`);
  if(!res.ok){
    tbodyEl.innerHTML = '<tr><td colspan="99">Fehler beim Laden der Tabelle</td></tr>';
    // Auch Overview leeren/setzen
    const ov = document.getElementById('overview');
    if (ov) ov.innerHTML = '<div class="col-12"><p>Keine Daten.</p></div>';   
    return;
  }

  tbodyEl.innerHTML = await res.text();

  // Prüfen, ob überhaupt Zeilen vorhanden sind
  const allGeraete = getAllGeraeteFromTable();
  if (allGeraete.length === 0) {
    // Keine Geräte → Meldung ins Diagramm setzen
    document.getElementById('plot-container').innerHTML = '<p>Keine Geräte für diesen Zeitraum.</p>';
  }

  // Zielbox leeren, damit keine Alt-Controls liegen bleiben
  const bottomBox = document.getElementById('dt-controls-bottom');
  if(bottomBox) bottomBox.innerHTML = '';

  // DataTables neu initialisieren (nur wenn es Zeilen gibt)
  if (allGeraete.length > 0) {
    tableInstance = $('#my-table').DataTable({
      paging:false,  
      //pageLength:5, 
      //lengthMenu:[5,10,15],
      info:false, 
      searching:false, 
      responsive:true, 
      destroy:true,
      scrollY:'208px', 
      scrollCollapse:false,
      dom:'rt<"dt-bottom"l p>',
      language:{ lengthMenu:"Show _MENU_" },
      pagingType:'simple_numbers',
      order: [[1, 'desc']],
      columnDefs: [
        {
          targets: 2, // Volumen-Spalte 
          render: function (data, type, row) {
            if (type === 'display' && data != null) {
              return parseInt(data).toLocaleString('de-DE');
            }
            return data;
          }
        },
        {
          targets: [3, 4], // Gewicht & Wdh Spalten
          render: function (data, type, row) {
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

    // Click-Handler auf Tabellenzeilen
    $('#my-table tbody').off('click', 'tr').on('click', 'tr', function(e){
      const g = this.dataset.geraet; if(!g) return;
      if(e.ctrlKey || e.metaKey){
        selectedGeraete.has(g) ? selectedGeraete.delete(g) : selectedGeraete.add(g);
      } else {
        selectedGeraete.clear(); selectedGeraete.add(g);
      }
      highlightSelection(); 
      fetchAndRenderPlot();
      markLongTextValues();
    });
  }

  // Falls noch keine Auswahl, aber Geräte da → alle auswählen
  if (selectedGeraete.size === 0 && allGeraete.length > 0) {
    selectedGeraete = new Set(allGeraete);
  }
  highlightSelection();

  await fetchAndRenderPlot();
  await fetchAndRenderOverview();
  markLongTextValues();
}

// ⬇️ NEU: Holt Overview-HTML von /api/gym-overview und rendert sie
async function fetchAndRenderOverview(){
  const container = document.getElementById('overview');
  if (!container) return;

  try{
    const res = await fetch(`/api/gym-overview?${buildParams({includeMetric:false, includeGeraete:true})}`);
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

// Holt JSON Daten von api/gym-plot
async function fetchAndRenderPlot(){
  const container = document.getElementById('plot-container');
  const res = await fetch(`/api/gym-plot?${buildParams({includeMetric:true, includeGeraete:true})}`);

  try{
    if(res.status===204){ container.innerHTML = '<p>Kein Diagramm für diese Auswahl.</p>'; return; }
    if(!res.ok){
      const t = await res.text().catch(()=> ''); console.error('[plot] HTTP error:', res.status, t);
      container.innerHTML = `<p style="color:#f88;">Fehler beim Laden des Diagramms (${res.status}).</p>`;
      return;
    }
    const fig = await res.json();
    container.innerHTML = '';
    if(Array.isArray(fig?.data) && fig.data.length>0){
      Plotly.newPlot(container, fig.data, fig.layout||{}, {responsive:true});
    }else{
      container.innerHTML = '<p>Kein Diagramm für diese Auswahl.</p>';
    }
  }catch(e){
    console.error('[plot] JS error:', e);
    container.innerHTML = '<p style="color:#f88;">Fehler beim Verarbeiten der Plot-Daten.</p>';
  }
}
