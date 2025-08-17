import { initWorkoutModal, initPersonSelectModal, initWorkoutFormSubmit } from './modal.js';
import { initFilters } from './api.js';

console.log("main.js loaded!");

document.addEventListener('DOMContentLoaded', () => {
  initWorkoutModal();
  initWorkoutFormSubmit();
  initFilters();
  initPersonSelectModal();
});
