console.log("gym.js loaded!");

let tableInstance = null;
let selectedGeraete = new Set(); // aktuell ausgewählte Geräte

document.addEventListener('DOMContentLoaded', () => {
  const form = document.querySelector('#filter-form');
  const metricSelect = document.getElementById('metric');

  // Grundfilter → Tabelle + Plot neu laden, Auswahl leeren
  form.querySelectorAll('#person, #gym, #start, #end').forEach(elem => {
    elem.addEventListener('change', async () => {
      selectedGeraete.clear();
      await fetchAndRender();
    });
  });

  // Metrik → nur Plot neu laden
  if (metricSelect) {
    metricSelect.addEventListener('change', async () => {
      await fetchAndRenderPlot();
    });
  }

  // Buttons (delegiert)
  $(document)
    .off('click', '#select-all')
    .on('click', '#select-all', e => {
      e.preventDefault();
      selectedGeraete = new Set(getAllGeraeteFromTable());
      highlightSelection();
      fetchAndRenderPlot();
    });

  $(document)
    .off('click', '#deselect-all')
    .on('click', '#deselect-all', e => {
      e.preventDefault();
      selectedGeraete.clear();
      highlightSelection();
      fetchAndRenderPlot();
    });

  // Initial laden
  (async () => {
    await fetchAndRender();
  })();
});

function getAllGeraeteFromTable() {
  const geraete = [];
  document.querySelectorAll('#my-table tbody tr').forEach(row => {
    if (row.dataset.geraet) {
      geraete.push(row.dataset.geraet);
    }
  });
  return geraete;
}

function highlightSelection() {
  document.querySelectorAll('#my-table tbody tr').forEach(row => {
    if (selectedGeraete.has(row.dataset.geraet)) {
      row.classList.add('table-active');
    } else {
      row.classList.remove('table-active');
    }
  });
}

/** Verschiebt "Show" & "Pagination" in den unteren Bereich der Sidebar */
function moveDTControlsToSidebarBottom() {
  if (!tableInstance) return;

  const $wrapper   = $(tableInstance.table().container());
  const $length    = $wrapper.find('.dataTables_length');   // "Show X"
  const $paginate  = $wrapper.find('.dataTables_paginate'); // Pagination
  const $bottomBox = $('#dt-controls-bottom');

  if (!$bottomBox.length) return;

  if ($length.length)   $length.appendTo($bottomBox);
  if ($paginate.length) $paginate.appendTo($bottomBox);

  // ursprüngliche dt-bottom-Leiste ausblenden, da wir Controls in der Sidebar haben
  $wrapper.find('.dt-bottom').hide();
}

async function fetchAndRender() {
  const form = document.querySelector('#filter-form');
  const formData = new FormData(form);
  const params = new URLSearchParams();

  for (const [key, value] of formData.entries()) {
    params.append(key, value);
  }

  const metricSelect = document.getElementById('metric');
  if (metricSelect && !params.has('metric')) {
    params.append('metric', metricSelect.value);
  }

  params.append('_ts', Date.now()); // cache bust

  const tableEl = document.getElementById('my-table');
  const tbodyEl = document.getElementById('table-body');

  // DataTable sauber zerstören, bevor wir DOM tauschen
  if ($.fn.DataTable.isDataTable(tableEl)) {
    $(tableEl).DataTable().clear().destroy();
  }
  tableInstance = null;

  const tableRes = await fetch(`/api/gym-table?${params.toString()}`);
  if (tableRes.ok) {
    const tableHtml = await tableRes.text();
    tbodyEl.innerHTML = tableHtml;

    // DataTables neu initialisieren
    tableInstance = $('#my-table').DataTable({
      paging: true,
      pageLength: 15,                  // dein Screenshot zeigt 16 – passe an, wie du magst
      lengthMenu: [5, 10, 15],
      info: false,
      searching: false,
      responsive: true,
      destroy: true,

      // fester Tabellenbereich mit Scroll
      scrollY: '265px',
      scrollCollapse: false,

      // DOM: l & p generieren lassen (wir verschieben sie dann)
      dom: 'rt<"dt-bottom"l p>',

      // "entries" entfernen
      language: {
        lengthMenu: "Show _MENU_"
      },
      // Pagination-Style
      pagingType: 'simple_numbers'
    });

    // Controls in den unteren Sidebar-Bereich schieben
    moveDTControlsToSidebarBottom();

    // Click-Handler auf Tabellenzeilen
    $('#my-table tbody').off('click', 'tr').on('click', 'tr', function (e) {
      const geraet = this.dataset.geraet;
      if (!geraet) return;

      if (e.ctrlKey || e.metaKey) {
        if (selectedGeraete.has(geraet)) selectedGeraete.delete(geraet);
        else selectedGeraete.add(geraet);
      } else {
        selectedGeraete.clear();
        selectedGeraete.add(geraet);
      }
      highlightSelection();
      fetchAndRenderPlot();
    });

    // Bei jedem redraw Auswahl hervorheben + Controls erneut verschieben
    tableInstance.on('draw', () => {
      highlightSelection();
      moveDTControlsToSidebarBottom();
    });

    // Falls noch keine Auswahl → alle auswählen
    if (selectedGeraete.size === 0) {
      selectedGeraete = new Set(getAllGeraeteFromTable());
    }
    highlightSelection();

    await fetchAndRenderPlot();
  } else {
    tbodyEl.innerHTML = '<tr><td colspan="99">Fehler beim Laden der Tabelle</td></tr>';
  }
}

async function fetchAndRenderPlot() {
  const form = document.querySelector('#filter-form');
  const formData = new FormData(form);
  const params = new URLSearchParams();

  for (const [key, value] of formData.entries()) {
    params.append(key, value);
  }

  const metricSelect = document.getElementById('metric');
  if (metricSelect && !params.has('metric')) {
    params.append('metric', metricSelect.value);
  }

  if (selectedGeraete.size > 0) {
    selectedGeraete.forEach(g => params.append('geraete', g));
  }

  params.append('_ts', Date.now()); // cache bust

  const plotRes = await fetch(`/api/gym-plot?${params.toString()}`);
  const container = document.getElementById('plot-container');

  try {
    if (plotRes.status === 204) {
      container.innerHTML = '<p>Kein Diagramm für diese Auswahl.</p>';
      return;
    }

    if (!plotRes.ok) {
      const errText = await plotRes.text().catch(() => '');
      console.error('[plot] HTTP error:', plotRes.status, errText);
      container.innerHTML = `<p style="color:#f88;">Fehler beim Laden des Diagramms (${plotRes.status}).</p>`;
      return;
    }

    const figData = await plotRes.json();
    container.innerHTML = '';
    if (figData && Array.isArray(figData.data) && figData.data.length > 0) {
      Plotly.newPlot(container, figData.data, figData.layout || {}, { responsive: true });
    } else {
      container.innerHTML = '<p>Kein Diagramm für diese Auswahl.</p>';
    }
  } catch (e) {
    console.error('[plot] JS error while handling response:', e);
    container.innerHTML = '<p style="color:#f88;">Fehler beim Verarbeiten der Plot-Daten.</p>';
  }
}
