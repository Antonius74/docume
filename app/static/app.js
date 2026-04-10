const state = {
  page: 1,
  pageSize: 40,
  selectedTheme: "",
  themes: [],
};

const elements = {
  modeFileBtn: document.getElementById("mode-file"),
  modeLinkBtn: document.getElementById("mode-link"),
  fileForm: document.getElementById("file-form"),
  linkForm: document.getElementById("link-form"),
  uploadStatus: document.getElementById("upload-status"),
  searchInput: document.getElementById("search-input"),
  sourceSelect: document.getElementById("source-select"),
  sortSelect: document.getElementById("sort-select"),
  refreshBtn: document.getElementById("refresh-btn"),
  clearFolderBtn: document.getElementById("clear-folder-btn"),
  folderList: document.getElementById("folder-list"),
  selectedFolderTitle: document.getElementById("selected-folder-title"),
  selectedFolderMeta: document.getElementById("selected-folder-meta"),
  recentList: document.getElementById("recent-list"),
  resultList: document.getElementById("result-list"),
  cardTemplate: document.getElementById("card-template"),
};

function setMode(mode) {
  const fileMode = mode === "file";
  elements.modeFileBtn.classList.toggle("active", fileMode);
  elements.modeLinkBtn.classList.toggle("active", !fileMode);
  elements.fileForm.classList.toggle("hidden", !fileMode);
  elements.linkForm.classList.toggle("hidden", fileMode);
}

function setStatus(message, isError = false) {
  elements.uploadStatus.textContent = message;
  elements.uploadStatus.classList.toggle("error", isError);
}

function compactPath(path) {
  if (!path) {
    return "";
  }
  const parts = path.split("/");
  if (parts.length <= 4) {
    return path;
  }
  return `.../${parts.slice(-4).join("/")}`;
}

function formatUploadDate(value) {
  const date = new Date(value);
  return date.toLocaleString();
}

function selectedThemeCount() {
  const found = state.themes.find((theme) => theme.theme === state.selectedTheme);
  return found ? found.count : 0;
}

function renderFolderList() {
  elements.folderList.innerHTML = "";

  if (!state.themes.length) {
    const empty = document.createElement("p");
    empty.className = "helper-text";
    empty.textContent = "Nessuna cartella disponibile.";
    elements.folderList.appendChild(empty);
    return;
  }

  for (const entry of state.themes) {
    const button = document.createElement("button");
    button.type = "button";
    button.className = "folder-item";
    if (state.selectedTheme === entry.theme) {
      button.classList.add("active");
    }

    const name = document.createElement("span");
    name.className = "folder-name";
    name.textContent = entry.theme;

    const count = document.createElement("span");
    count.className = "folder-count";
    count.textContent = String(entry.count);

    button.appendChild(name);
    button.appendChild(count);

    button.addEventListener("click", () => {
      state.selectedTheme = entry.theme;
      renderFolderList();
      loadResources().catch((err) => setStatus(err.message, true));
    });

    elements.folderList.appendChild(button);
  }
}

function renderFolderHeader() {
  if (!state.selectedTheme) {
    elements.selectedFolderTitle.textContent = "Seleziona una cartella";
    elements.selectedFolderMeta.textContent = "Clicca su una cartella a sinistra per vedere i file.";
    return;
  }

  elements.selectedFolderTitle.textContent = state.selectedTheme;
  elements.selectedFolderMeta.textContent = `${selectedThemeCount()} file/link nella cartella`;
}

async function loadThemes() {
  const response = await fetch("/api/themes");
  if (!response.ok) {
    throw new Error("Impossibile caricare le cartelle");
  }

  state.themes = await response.json();

  if (state.selectedTheme && !state.themes.some((theme) => theme.theme === state.selectedTheme)) {
    state.selectedTheme = "";
  }

  renderFolderList();
  renderFolderHeader();
}

function buildResourcesQuery() {
  const params = new URLSearchParams();
  const q = elements.searchInput.value.trim();
  const sourceType = elements.sourceSelect.value;

  if (state.selectedTheme) {
    params.set("theme", state.selectedTheme);
  }
  if (q) {
    params.set("q", q);
  }
  if (sourceType) {
    params.set("source_type", sourceType);
  }

  params.set("sort_by", elements.sortSelect.value);
  params.set("order", "desc");
  params.set("semantic", "true");
  params.set("page", String(state.page));
  params.set("page_size", String(state.pageSize));

  return params.toString();
}

function renderEmptyFilesState(message) {
  elements.resultList.innerHTML = "";
  const empty = document.createElement("p");
  empty.className = "helper-text helper-card";
  empty.textContent = message;
  elements.resultList.appendChild(empty);
}

function renderCards(items) {
  elements.resultList.innerHTML = "";

  if (!items.length) {
    renderEmptyFilesState("Nessun file trovato nella cartella selezionata.");
    return;
  }

  for (const item of items) {
    const node = elements.cardTemplate.content.cloneNode(true);

    const canonicalTheme = item.canonical_theme || item.inferred_theme || "General";
    node.querySelector(".badge.category").textContent = canonicalTheme;

    const rawThemeBadge = node.querySelector(".badge.theme");
    if (item.inferred_theme && item.inferred_theme !== canonicalTheme) {
      rawThemeBadge.textContent = `Orig: ${item.inferred_theme}`;
    } else {
      rawThemeBadge.remove();
    }

    node.querySelector(".badge.type").textContent = item.source_type.toUpperCase();
    node.querySelector(".title").textContent = item.title;
    node.querySelector(".summary").textContent = item.summary || "Nessuna sintesi disponibile.";

    const uploadDateNode = node.querySelector(".upload-date");
    uploadDateNode.textContent = `Data caricamento: ${formatUploadDate(item.uploaded_at)}`;

    const actions = node.querySelector(".actions");

    if (item.source_url) {
      const link = document.createElement("a");
      link.href = item.source_url;
      link.target = "_blank";
      link.rel = "noopener noreferrer";
      link.textContent = "Apri sorgente";
      actions.appendChild(link);
    }

    if (item.source_type === "file") {
      const fileLink = document.createElement("a");
      fileLink.href = `/api/files/${item.id}`;
      fileLink.textContent = "Scarica file";
      actions.appendChild(fileLink);
    }

    const deleteBtn = document.createElement("button");
    deleteBtn.type = "button";
    deleteBtn.className = "action-delete";
    deleteBtn.textContent = "Elimina";
    deleteBtn.addEventListener("click", () => {
      deleteResource(item).catch((err) => setStatus(err.message, true));
    });
    actions.appendChild(deleteBtn);

    elements.resultList.appendChild(node);
  }
}

async function deleteResource(item) {
  const label = item.title || item.id;
  const confirmed = window.confirm(`Eliminare definitivamente "${label}"?`);
  if (!confirmed) {
    return;
  }

  setStatus(`Eliminazione in corso: ${label}...`);
  const response = await fetch(`/api/resources/${item.id}`, { method: "DELETE" });
  if (!response.ok) {
    const err = await response.json().catch(() => ({ detail: "Errore sconosciuto" }));
    throw new Error(err.detail || "Eliminazione fallita");
  }

  setStatus(`Eliminato: ${label}`);
  await Promise.all([loadThemes(), loadRecent()]);
  await loadResources();
}

async function loadResources() {
  renderFolderHeader();

  if (!state.selectedTheme) {
    renderEmptyFilesState("Seleziona una cartella per visualizzare i file.");
    return;
  }

  const query = buildResourcesQuery();
  const response = await fetch(`/api/resources?${query}`);
  if (!response.ok) {
    throw new Error("Impossibile caricare i file della cartella");
  }

  const data = await response.json();
  renderCards(data.items || []);
}

async function loadRecent() {
  const response = await fetch("/api/resources/recent?limit=8");
  if (!response.ok) {
    throw new Error("Impossibile caricare gli ultimi file");
  }

  const items = await response.json();
  elements.recentList.innerHTML = "";

  if (!items.length) {
    const empty = document.createElement("li");
    empty.className = "recent-empty";
    empty.textContent = "Nessun contenuto caricato.";
    elements.recentList.appendChild(empty);
    return;
  }

  for (const item of items) {
    const li = document.createElement("li");
    li.className = "recent-item";

    const top = document.createElement("div");
    top.className = "recent-top";

    const title = document.createElement("strong");
    title.textContent = item.title;

    const type = document.createElement("span");
    type.className = "recent-type";
    type.textContent = item.source_type.toUpperCase();

    top.appendChild(title);
    top.appendChild(type);

    const meta = document.createElement("p");
    meta.className = "recent-meta";
    meta.textContent = `${item.canonical_theme || item.inferred_theme || "General"} · Caricato: ${formatUploadDate(item.uploaded_at)}`;

    const action = document.createElement("a");
    if (item.source_type === "file") {
      action.href = `/api/files/${item.id}`;
      action.textContent = "Apri file";
    } else {
      action.href = item.source_url || "#";
      action.target = "_blank";
      action.rel = "noopener noreferrer";
      action.textContent = "Apri link";
    }

    li.appendChild(top);
    li.appendChild(meta);
    li.appendChild(action);
    elements.recentList.appendChild(li);
  }
}

async function submitFile(event) {
  event.preventDefault();
  setStatus("Caricamento file e classificazione in corso...");

  const formData = new FormData(elements.fileForm);
  const response = await fetch("/api/ingest/file", {
    method: "POST",
    body: formData,
  });

  if (!response.ok) {
    const err = await response.json().catch(() => ({ detail: "Errore sconosciuto" }));
    throw new Error(err.detail || "Upload file fallito");
  }

  const resource = await response.json();
  setStatus(`Caricato: ${resource.title} (${resource.canonical_theme || resource.inferred_theme})`);
  elements.fileForm.reset();

  await Promise.all([loadThemes(), loadRecent()]);
  await loadResources();
}

async function submitLink(event) {
  event.preventDefault();
  setStatus("Salvataggio link e classificazione in corso...");

  const formData = new FormData(elements.linkForm);
  const payload = {
    url: formData.get("url"),
    title: formData.get("title") || null,
    description: formData.get("description") || null,
  };

  const response = await fetch("/api/ingest/link", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify(payload),
  });

  if (!response.ok) {
    const err = await response.json().catch(() => ({ detail: "Errore sconosciuto" }));
    throw new Error(err.detail || "Upload link fallito");
  }

  const resource = await response.json();
  setStatus(`Link salvato: ${resource.title} (${resource.canonical_theme || resource.inferred_theme})`);
  elements.linkForm.reset();

  await Promise.all([loadThemes(), loadRecent()]);
  await loadResources();
}

async function refreshAll() {
  await Promise.all([loadThemes(), loadRecent()]);
  await loadResources();
}

function debounce(callback, timeout = 300) {
  let timer;
  return (...args) => {
    clearTimeout(timer);
    timer = setTimeout(() => callback(...args), timeout);
  };
}

const debouncedSearch = debounce(() => {
  state.page = 1;
  loadResources().catch((err) => setStatus(err.message, true));
}, 300);

async function bootstrap() {
  setMode("file");
  await Promise.all([loadThemes(), loadRecent()]);
  await loadResources();
}

elements.modeFileBtn.addEventListener("click", () => setMode("file"));
elements.modeLinkBtn.addEventListener("click", () => setMode("link"));

elements.fileForm.addEventListener("submit", (event) => {
  submitFile(event).catch((err) => setStatus(err.message, true));
});
elements.linkForm.addEventListener("submit", (event) => {
  submitLink(event).catch((err) => setStatus(err.message, true));
});

elements.refreshBtn.addEventListener("click", () => {
  refreshAll().catch((err) => setStatus(err.message, true));
});

elements.clearFolderBtn.addEventListener("click", () => {
  state.selectedTheme = "";
  renderFolderList();
  loadResources().catch((err) => setStatus(err.message, true));
});

elements.searchInput.addEventListener("input", debouncedSearch);
elements.sourceSelect.addEventListener("change", debouncedSearch);
elements.sortSelect.addEventListener("change", debouncedSearch);

bootstrap().catch((err) => setStatus(err.message, true));
