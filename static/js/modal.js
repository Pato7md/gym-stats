import { fetchAndRenderAll } from './api.js';


export function initPersonSelectModal() {
    const personStartModal = new bootstrap.Modal(document.getElementById('personModalStart'), {
        backdrop: 'static',
        keyboard: false
    });
    
    const confirmBtn = document.getElementById('confirm-person');
    const personSelectModal = document.getElementById('person-select-modal');
    const welcomeUser = document.getElementById('welcome-user'); // <--- das hat dir gefehlt!

    personStartModal.show();

    confirmBtn.addEventListener('click', async () => {
        if (!personSelectModal.value) {
        alert("Bitte zuerst eine Person wählen.");
        return;
        }

        if (welcomeUser) {
          welcomeUser.textContent = "Welcome, " + personSelectModal.value;
        }

        await fetchAndRenderAll();
        personStartModal.hide();
    });
}


export function initWorkoutModal() {
    const modal = document.getElementById("inputModal");
    const openBtn = document.getElementById("openModalBtn");
    const closeBtn = document.getElementById("closeModalBtn");

    const saetzeInput = document.getElementById("saetze");
    const saetzeContainer = document.getElementById("saetze-container");
  
    if (saetzeInput && saetzeContainer) {
      saetzeInput.addEventListener("input", () => {
        const count = parseInt(saetzeInput.value) || 0;
        saetzeContainer.innerHTML = ""; // reset
  
        for (let i = 1; i <= count; i++) {
          saetzeContainer.innerHTML += `
            <div class="mb-3">
              <label class="form-label fw-bold">Satz ${i}</label>
              <div class="row">
                <div class="col">
                  <label for="satz${i}_gew" class="form-label small">Gewicht</label>
                  <input type="number" step="0.5" id="satz${i}_gew" name="satz${i}_gew" class="form-control">
                </div>
                <div class="col">
                  <label for="satz${i}_wdh" class="form-label small">Wdh</label>
                  <input type="number" id="satz${i}_wdh" name="satz${i}_wdh" class="form-control">
                </div>
              </div>
            </div>
          `;
        }
      });
    }
  
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


export function initWorkoutFormSubmit() {
    const modal = document.getElementById("inputModal");
    const workoutForm = document.getElementById("workoutForm");
  
    if (!workoutForm) return;
  
    workoutForm.addEventListener("submit", async (e) => {
      e.preventDefault();
  
      const saetzeCount = parseInt(document.getElementById("saetze").value)
      const saetze =  [];
  
      for (let i = 1; i <= saetzeCount; i++) {
        const gewicht = parseFloat(document.getElementById(`satz${i}_gew`)?.value) || null;
        const wdh     = parseInt(document.getElementById(`satz${i}_wdh`)?.value) || null;  
      
        saetze.push({satz: i, gewicht, wdh});
      }
  
      const formData = {
        person: document.getElementById("person-select-modal").value.trim(),
        gym: document.getElementById("gymModal").value.trim(),
        geraet: document.getElementById("geraet").value.trim(),
        saetze: saetzeCount,
        datum: document.getElementById("date").value,
        details: saetze
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
          await fetchAndRenderAll();
        } else {
          alert("Fehler beim Speichern: " + (data.error || "unbekannt"));
        }
      } catch (err) {
        console.error("Fehler beim Senden:", err);
        alert("Verbindungsfehler beim Speichern");
      }
    });
}
    

export function initMenuModal() {
  const menuBtn = document.getElementById("menu-btn");
  const centralMenu = document.getElementById("central-menu");

  if (!menuBtn || !centralMenu) return;

  menuBtn.addEventListener("click", () => {
    centralMenu.classList.remove("d-none");
  });

  centralMenu.addEventListener("click", (e) => {
    if (e.target === centralMenu) {
      centralMenu.classList.add("d-none");
    }
  });
}
