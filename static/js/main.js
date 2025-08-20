import { initWorkoutModal, initPersonSelectModal, initMenuModal, initWorkoutFormSubmit } from './modal.js';
import { initFilterRefresh } from './api.js';

console.log("main.js loaded!");

document.addEventListener('DOMContentLoaded', () => {
  initWorkoutModal();
  initWorkoutFormSubmit();
  initFilterRefresh();
  initPersonSelectModal();
  initMenuModal();
});
