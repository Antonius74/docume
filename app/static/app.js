const state = {
  page: 1,
  pageSize: 40,
  selectedTheme: "",
  selectedAuthor: "",
  selectedDetail: "",
  themes: [],
  themeTree: [],
  authors: [],
};

const elements = {
  modeFileBtn: document.getElementById("mode-file"),
  modeLinkBtn: document.getElementById("mode-link"),
  fileForm: document.getElementById("file-form"),
  linkForm: document.getElementById("link-form"),
  uploadStatus: document.getElementById("upload-status"),
  searchInput: document.getElementById("search-input"),
  sourceSelect: document.getElementById("source-select"),
  authorSelect: document.getElementById("author-select"),
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

function sanitizeTooltipText(value) {
  if (!value) {
    return "";
  }
  return String(value)
    .replace(/\x00/g, " ")
    .replace(/\s+/g, " ")
    .replace(/^\[preview[^\]]*\]\s*/i, "")
    .trim();
}

function shortText(value, maxLen = 180) {
  const clean = sanitizeTooltipText(value);
  if (!clean) {
    return "";
  }
  if (clean.length <= maxLen) {
    return clean;
  }
  return `${clean.slice(0, maxLen - 1).trimEnd()}…`;
}

function tooltipDescription(item) {
  const summary = shortText(item.summary, 220);
  if (summary) {
    return summary;
  }

  const description = shortText(item.description, 220);
  if (description) {
    return description;
  }

  if (item.source_type === "link" && item.source_url) {
    try {
      const host = new URL(item.source_url).hostname.replace(/^www\./i, "");
      return `Contenuto dal sito ${host}.`;
    } catch (_) {
      return "Contenuto web senza descrizione disponibile.";
    }
  }

  const mime = sanitizeTooltipText(item.mime_type || "");
  if (mime) {
    return `Documento ${mime}.`;
  }
  return "Nessuna descrizione disponibile.";
}

function sourceInfo(item) {
  if (item.source_type === "link") {
    const url = (item.source_url || "").toLowerCase();
    const isYoutube =
      Boolean(item.youtube_video_id) ||
      url.includes("youtube.com") ||
      url.includes("youtu.be");
    if (isYoutube) {
      return { label: "YT", kind: "yt" };
    }
    return { label: "Sito Web", kind: "web" };
  }

  const mime = (item.mime_type || "").toLowerCase();
  if (mime.startsWith("image/")) {
    return { label: "Immagine", kind: "image" };
  }
  if (mime.startsWith("audio/")) {
    return { label: "Audio", kind: "audio" };
  }
  if (mime.startsWith("video/")) {
    return { label: "Video", kind: "video" };
  }
  return { label: "Documento", kind: "doc" };
}

function selectedThemeCount() {
  const found = state.themeTree.find((theme) => theme.theme === state.selectedTheme);
  return found ? found.count : 0;
}

function selectedScopeCount() {
  const themeNode = state.themeTree.find((theme) => theme.theme === state.selectedTheme);
  if (!themeNode) {
    return 0;
  }
  if (!state.selectedAuthor) {
    return themeNode.count || 0;
  }
  const authorNode = (themeNode.authors || []).find((author) => author.author === state.selectedAuthor);
  if (!authorNode) {
    return themeNode.count || 0;
  }
  if (!state.selectedDetail) {
    return authorNode.count || 0;
  }
  const detailNode = (authorNode.details || []).find((detail) => detail.detail === state.selectedDetail);
  return detailNode ? detailNode.count || 0 : authorNode.count || 0;
}

function themePathLabel() {
  const parts = [state.selectedTheme, state.selectedAuthor, state.selectedDetail].filter(Boolean);
  return parts.join(" -> ");
}

function renderAuthorSelect() {
  const select = elements.authorSelect;
  select.innerHTML = "";

  const all = document.createElement("option");
  all.value = "";
  all.textContent = "Tutti gli autori";
  select.appendChild(all);

  for (const entry of state.authors) {
    const option = document.createElement("option");
    option.value = entry.author;
    option.textContent = `${entry.author} (${entry.count})`;
    if (state.selectedAuthor === entry.author) {
      option.selected = true;
    }
    select.appendChild(option);
  }
}

function renderFolderList() {
  elements.folderList.innerHTML = "";

  if (!state.themeTree.length) {
    const empty = document.createElement("p");
    empty.className = "helper-text";
    empty.textContent = "Nessuna cartella disponibile.";
    elements.folderList.appendChild(empty);
    return;
  }

  for (const themeNode of state.themeTree) {
    const branch = document.createElement("div");
    branch.className = "folder-branch";

    const themeBtn = document.createElement("button");
    themeBtn.type = "button";
    themeBtn.className = "folder-item level-theme";
    if (state.selectedTheme === themeNode.theme && !state.selectedAuthor && !state.selectedDetail) {
      themeBtn.classList.add("active");
    }
    themeBtn.innerHTML = `<span class="folder-name">${themeNode.theme}</span><span class="folder-count">${themeNode.count}</span>`;
    themeBtn.addEventListener("click", () => {
      state.selectedTheme = themeNode.theme;
      state.selectedAuthor = "";
      state.selectedDetail = "";
      renderFolderList();
      renderFolderHeader();
      Promise.all([loadAuthors(), loadResources()]).catch((err) => setStatus(err.message, true));
    });
    branch.appendChild(themeBtn);

    const authorsWrap = document.createElement("div");
    authorsWrap.className = "folder-children";
    for (const authorNode of themeNode.authors || []) {
      const authorBtn = document.createElement("button");
      authorBtn.type = "button";
      authorBtn.className = "folder-item level-author";
      if (
        state.selectedTheme === themeNode.theme &&
        state.selectedAuthor === authorNode.author &&
        !state.selectedDetail
      ) {
        authorBtn.classList.add("active");
      }
      authorBtn.innerHTML = `<span class="folder-name">${authorNode.author}</span><span class="folder-count">${authorNode.count}</span>`;
      authorBtn.addEventListener("click", () => {
        state.selectedTheme = themeNode.theme;
        state.selectedAuthor = authorNode.author;
        state.selectedDetail = "";
        renderFolderList();
        renderFolderHeader();
        Promise.all([loadAuthors(), loadResources()]).catch((err) => setStatus(err.message, true));
      });
      authorsWrap.appendChild(authorBtn);

      const detailsWrap = document.createElement("div");
      detailsWrap.className = "folder-children details";
      for (const detailNode of authorNode.details || []) {
        const detailBtn = document.createElement("button");
        detailBtn.type = "button";
        detailBtn.className = "folder-item level-detail";
        if (
          state.selectedTheme === themeNode.theme &&
          state.selectedAuthor === authorNode.author &&
          state.selectedDetail === detailNode.detail
        ) {
          detailBtn.classList.add("active");
        }
        detailBtn.innerHTML = `<span class="folder-name">${detailNode.detail}</span><span class="folder-count">${detailNode.count}</span>`;
        detailBtn.addEventListener("click", () => {
          state.selectedTheme = themeNode.theme;
          state.selectedAuthor = authorNode.author;
          state.selectedDetail = detailNode.detail;
          renderFolderList();
          renderFolderHeader();
          Promise.all([loadAuthors(), loadResources()]).catch((err) => setStatus(err.message, true));
        });
        detailsWrap.appendChild(detailBtn);
      }
      if ((authorNode.details || []).length) {
        authorsWrap.appendChild(detailsWrap);
      }
    }
    if ((themeNode.authors || []).length) {
      branch.appendChild(authorsWrap);
    }

    elements.folderList.appendChild(branch);
  }
}

function renderFolderHeader() {
  const hasQuery = elements.searchInput.value.trim().length > 0;

  if (!state.selectedTheme) {
    if (hasQuery) {
      elements.selectedFolderTitle.textContent = "Ricerca globale";
      elements.selectedFolderMeta.textContent = "Risultati dal database su tutti i contenuti.";
    } else {
      elements.selectedFolderTitle.textContent = "Seleziona una cartella";
      elements.selectedFolderMeta.textContent = "Clicca su una cartella a sinistra per vedere i file.";
    }
    return;
  }

  elements.selectedFolderTitle.textContent = state.selectedTheme;
  const path = themePathLabel();
  const pathText = path ? ` · Percorso: ${path}` : "";
  elements.selectedFolderMeta.textContent = `${selectedScopeCount()} file/link nella selezione${pathText}`;
}

async function loadThemeTree() {
  const params = new URLSearchParams();
  const sourceType = elements.sourceSelect.value;
  if (sourceType) {
    params.set("source_type", sourceType);
  }
  const suffix = params.toString();
  const response = await fetch(`/api/theme-tree${suffix ? `?${suffix}` : ""}`);
  if (!response.ok) {
    throw new Error("Impossibile caricare le cartelle");
  }

  state.themeTree = await response.json();
  state.themes = (state.themeTree || []).map((node) => ({ theme: node.theme, count: node.count }));

  if (state.selectedTheme && !state.themeTree.some((theme) => theme.theme === state.selectedTheme)) {
    state.selectedTheme = "";
    state.selectedAuthor = "";
    state.selectedDetail = "";
  }

  const currentTheme = state.themeTree.find((theme) => theme.theme === state.selectedTheme);
  if (
    currentTheme &&
    state.selectedAuthor &&
    !(currentTheme.authors || []).some((author) => author.author === state.selectedAuthor)
  ) {
    state.selectedAuthor = "";
    state.selectedDetail = "";
  }

  const currentAuthor = (currentTheme?.authors || []).find((author) => author.author === state.selectedAuthor);
  if (
    currentAuthor &&
    state.selectedDetail &&
    !(currentAuthor.details || []).some((detail) => detail.detail === state.selectedDetail)
  ) {
    state.selectedDetail = "";
  }

  renderFolderList();
  renderFolderHeader();
}

async function loadThemes() {
  await loadThemeTree();
}

async function loadAuthors() {
  const params = new URLSearchParams();
  if (state.selectedTheme) {
    params.set("theme", state.selectedTheme);
  }
  const sourceType = elements.sourceSelect.value;
  if (sourceType) {
    params.set("source_type", sourceType);
  }
  const suffix = params.toString();
  const response = await fetch(`/api/authors${suffix ? `?${suffix}` : ""}`);
  if (!response.ok) {
    throw new Error("Impossibile caricare gli autori");
  }
  state.authors = await response.json();
  if (state.selectedAuthor && !state.authors.some((item) => item.author === state.selectedAuthor)) {
    state.selectedAuthor = "";
    state.selectedDetail = "";
  }
  renderAuthorSelect();
}

function buildResourcesQuery() {
  const params = new URLSearchParams();
  const q = elements.searchInput.value.trim();
  const sourceType = elements.sourceSelect.value;

  if (state.selectedTheme) {
    params.set("theme", state.selectedTheme);
  }
  if (state.selectedAuthor) {
    params.set("author", state.selectedAuthor);
  }
  if (state.selectedDetail) {
    params.set("detail", state.selectedDetail);
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

    const typeBadge = node.querySelector(".badge.type");
    const source = sourceInfo(item);
    typeBadge.textContent = source.label;
    typeBadge.classList.add(`source-${source.kind}`);
    const authorName = sanitizeTooltipText(item.author_name || "") || "Sconosciuto";
    const authorBadge = document.createElement("span");
    authorBadge.className = "badge author";
    authorBadge.textContent = authorName;
    node.querySelector(".card-top").appendChild(authorBadge);
    const titleNode = node.querySelector(".title");
    titleNode.textContent = item.title;

    const titleRow = document.createElement("div");
    titleRow.className = "title-row";
    titleNode.replaceWith(titleRow);
    titleRow.appendChild(titleNode);

    const infoBtn = document.createElement("button");
    infoBtn.type = "button";
    infoBtn.className = "info-btn";
    infoBtn.textContent = "i";
    infoBtn.title = tooltipDescription(item);
    infoBtn.setAttribute("aria-label", "Mostra descrizione breve");
    titleRow.appendChild(infoBtn);

    node.querySelector(".summary").remove();
    const authorLineNode = node.querySelector(".author-line");
    authorLineNode.textContent = `Autore: ${authorName}`;

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
    deleteBtn.className = "action-delete-link";
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
  await loadThemes();
  await loadAuthors();
  await loadRecent();
  await loadResources();
}

async function loadResources() {
  renderFolderHeader();
  const hasQuery = elements.searchInput.value.trim().length > 0;

  if (!state.selectedTheme && !hasQuery) {
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
    const authorName = sanitizeTooltipText(item.author_name || "") || "Sconosciuto";
    meta.textContent = `${item.canonical_theme || item.inferred_theme || "General"} · ${authorName} · Caricato: ${formatUploadDate(item.uploaded_at)}`;

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

  await loadThemes();
  await loadAuthors();
  await loadRecent();
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

  await loadThemes();
  await loadAuthors();
  await loadRecent();
  await loadResources();
}

async function refreshAll() {
  await loadThemes();
  await loadAuthors();
  await loadRecent();
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
  await loadThemes();
  await loadAuthors();
  await loadRecent();
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
  state.selectedAuthor = "";
  renderFolderList();
  Promise.all([loadAuthors(), loadResources()]).catch((err) => setStatus(err.message, true));
});

elements.searchInput.addEventListener("input", debouncedSearch);
elements.sourceSelect.addEventListener("change", debouncedSearch);
elements.authorSelect.addEventListener("change", () => {
  state.selectedAuthor = elements.authorSelect.value;
  debouncedSearch();
});
elements.sortSelect.addEventListener("change", debouncedSearch);

bootstrap().catch((err) => setStatus(err.message, true));
