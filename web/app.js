const serverListEl = document.getElementById("serverList");
const serverItemTemplate = document.getElementById("serverItemTemplate");
const serverCountEl = document.getElementById("serverCount");
const filterListEl = document.getElementById("filterList");
const selectAllBtn = document.getElementById("selectAllBtn");
const clearAllBtn = document.getElementById("clearAllBtn");
const filterToggleBtn = document.getElementById("filterToggleBtn");
const filterPopoverEl = document.getElementById("filterPopover");
const refreshBtn = document.getElementById("refreshBtn");
const lastUpdatedEl = document.getElementById("lastUpdated");
const configPathEl = document.getElementById("configPath");
const statusTextEl = document.getElementById("statusText");
const emptyStateEl = document.getElementById("emptyState");
const detailPaneEl = document.getElementById("detailPane");
const detailEmptyEl = document.getElementById("detailEmpty");
const detailBodyEl = document.getElementById("detailBody");
const detailHostEl = document.getElementById("detailHost");
const detailPillEl = document.getElementById("detailPill");
const detailMetricCountEl = document.getElementById("detailMetricCount");
const detailMetricUtilEl = document.getElementById("detailMetricUtil");
const detailMetricMemEl = document.getElementById("detailMetricMem");
const detailGpuListEl = document.getElementById("detailGpuList");
const detailErrorEl = document.getElementById("detailError");
const commandBtn = document.getElementById("commandBtn");
const transferBtn = document.getElementById("transferBtn");
const transferBackdropEl = document.getElementById("transferBackdrop");
const transferModalEl = document.getElementById("transferModal");
const transferCloseBtn = document.getElementById("transferCloseBtn");
const transferHostLabelEl = document.getElementById("transferHostLabel");
const commandBackdropEl = document.getElementById("commandBackdrop");
const commandModalEl = document.getElementById("commandModal");
const commandCloseBtn = document.getElementById("commandCloseBtn");
const commandHostLabelEl = document.getElementById("commandHostLabel");
const commandScreenEl = document.getElementById("commandScreen");
const commandPromptEl = document.getElementById("commandPrompt");
const commandInputEl = document.getElementById("commandInput");
const commandRunBtn = document.getElementById("commandRunBtn");
const commandClearBtn = document.getElementById("commandClearBtn");
const commandStatusEl = document.getElementById("commandStatus");
const commandExitEl = document.getElementById("commandExit");
const commandOutputEl = document.getElementById("commandOutput");
const uploadTabBtn = document.getElementById("uploadTabBtn");
const downloadTabBtn = document.getElementById("downloadTabBtn");
const uploadPanelEl = document.getElementById("uploadPanel");
const downloadPanelEl = document.getElementById("downloadPanel");
const uploadDropzone = document.getElementById("uploadDropzone");
const uploadDropText = document.getElementById("uploadDropText");
const uploadFileInput = document.getElementById("uploadFileInput");
const uploadPathInput = document.getElementById("uploadPathInput");
const uploadStartBtn = document.getElementById("uploadStartBtn");
const uploadProgressBar = document.getElementById("uploadProgressBar");
const uploadPercentEl = document.getElementById("uploadPercent");
const uploadSpeedEl = document.getElementById("uploadSpeed");
const uploadStatusEl = document.getElementById("uploadStatus");
const downloadPathInput = document.getElementById("downloadPathInput");
const downloadNameInput = document.getElementById("downloadNameInput");
const downloadStartBtn = document.getElementById("downloadStartBtn");
const downloadProgressBar = document.getElementById("downloadProgressBar");
const downloadPercentEl = document.getElementById("downloadPercent");
const downloadSpeedEl = document.getElementById("downloadSpeed");
const downloadStatusEl = document.getElementById("downloadStatus");
const processPanelEl = document.getElementById("processPanel");
const processTitleEl = document.getElementById("processTitle");
const processSubtitleEl = document.getElementById("processSubtitle");
const processEmptyEl = document.getElementById("processEmpty");
const processListEl = document.getElementById("processList");
const processErrorEl = document.getElementById("processError");
const toastContainerEl = document.getElementById("toastContainer");
const startupPanelEl = document.getElementById("startupPanel");
const startupToggleEl = document.getElementById("startupToggle");
const startupStatusEl = document.getElementById("startupStatus");
const startupHintEl = document.getElementById("startupHint");

const REFRESH_MS = 30000;
let hosts = [];
let selectedHost = null;
let selectedGpuIndex = null;
let refreshTimer = null;
let selectedLoadedAt = 0;
let processLoadedAt = 0;
const serverItems = new Map();
const serverStatuses = new Map();
let detailHasData = false;
let processHasData = false;
let visibleHosts = new Set();
let manualFilter = false;
let uploadInProgress = false;
let downloadInProgress = false;
let commandInProgress = false;
const commandSessions = new Map();
let startupUpdating = false;

const formatPercent = (value) => `${value}%`;
const formatMiB = (value) => `${value.toLocaleString("en-US")} MiB`;
const formatOptionalMiB = (value) => (value == null ? "--" : formatMiB(value));

function formatBytesPerSecond(value) {
  if (!value || !Number.isFinite(value)) {
    return "--";
  }
  const kb = value / 1024;
  const mb = kb / 1024;
  if (mb >= 1) {
    return `${mb.toFixed(1)} MB/s`;
  }
  if (kb >= 1) {
    return `${kb.toFixed(1)} KB/s`;
  }
  return `${Math.round(value)} B/s`;
}

function formatFileSize(bytes) {
  if (!bytes || !Number.isFinite(bytes)) {
    return "--";
  }
  const kb = bytes / 1024;
  const mb = kb / 1024;
  if (mb >= 1) {
    return `${mb.toFixed(1)} MB`;
  }
  if (kb >= 1) {
    return `${kb.toFixed(1)} KB`;
  }
  return `${Math.round(bytes)} B`;
}

function getBaseName(path) {
  if (!path) {
    return "";
  }
  const cleaned = path.replace(/\\+/g, "/");
  const parts = cleaned.split("/");
  return parts[parts.length - 1] || "";
}

function formatTime(date) {
  return new Intl.DateTimeFormat("en-US", {
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
  }).format(date);
}

function scrollTerminalToBottom() {
  if (!commandOutputEl) {
    return;
  }
  commandOutputEl.scrollTop = commandOutputEl.scrollHeight;
}

function resizeCommandInput() {
  if (!commandInputEl) {
    return;
  }
  commandInputEl.style.height = "auto";
  commandInputEl.style.height = `${commandInputEl.scrollHeight}px`;
}

function getCommandSession(host) {
  if (!host) {
    return null;
  }
  if (!commandSessions.has(host)) {
    commandSessions.set(host, {
      cwd: "",
      history: [],
      historyIndex: -1,
      buffer: "",
    });
  }
  return commandSessions.get(host);
}

function formatPrompt(host, cwd) {
  const displayPath = cwd || "~";
  return `${host}:${displayPath}$`;
}

function setCommandPrompt(host, cwd) {
  if (!commandPromptEl) {
    return;
  }
  commandPromptEl.textContent = formatPrompt(host, cwd);
}

function setCommandOutput(text) {
  if (commandOutputEl) {
    commandOutputEl.textContent = text;
    scrollTerminalToBottom();
  }
}

function appendCommandOutput(session, text) {
  if (!session) {
    return;
  }
  const normalized = text ? text.replace(/\r\n/g, "\n") : "";
  const next = session.buffer ? `${session.buffer}\n${normalized}` : normalized;
  const limit = 60000;
  session.buffer = next.length > limit ? next.slice(-limit) : next;
  setCommandOutput(session.buffer || "Output will appear here.");
}

function longestCommonPrefix(list) {
  if (!list.length) {
    return "";
  }
  let prefix = list[0];
  for (let i = 1; i < list.length; i += 1) {
    const item = list[i];
    while (item.indexOf(prefix) !== 0) {
      prefix = prefix.slice(0, -1);
      if (!prefix) {
        return "";
      }
    }
  }
  return prefix;
}

function setStatus(message) {
  statusTextEl.textContent = message;
}

function setStartupVisible(visible) {
  if (startupPanelEl) {
    startupPanelEl.style.display = visible ? "flex" : "none";
  }
  if (startupHintEl) {
    startupHintEl.style.display = visible ? "block" : "none";
  }
}

function setStartupState(enabled, label) {
  if (startupStatusEl) {
    startupStatusEl.textContent = label;
  }
  if (startupToggleEl) {
    startupToggleEl.checked = !!enabled;
  }
}

function setStartupBusy(isBusy) {
  startupUpdating = isBusy;
  if (startupToggleEl) {
    startupToggleEl.disabled = isBusy;
  }
}

function setLastUpdated() {
  lastUpdatedEl.textContent = formatTime(new Date());
}

function showToast(message, type = "error") {
  if (!message || !toastContainerEl) {
    return;
  }
  const toast = document.createElement("div");
  toast.className = `toast ${type}`;
  toast.setAttribute("role", "alert");
  toast.textContent = message;
  toastContainerEl.appendChild(toast);
  requestAnimationFrame(() => toast.classList.add("show"));
  const timer = setTimeout(() => hideToast(toast), 5000);
  toast.addEventListener("click", () => {
    clearTimeout(timer);
    hideToast(toast);
  });
}

function hideToast(toast) {
  if (!toast) {
    return;
  }
  toast.classList.remove("show");
  toast.classList.add("hide");
  setTimeout(() => toast.remove(), 250);
}

function openFilterPopover() {
  if (!filterPopoverEl) {
    return;
  }
  filterPopoverEl.classList.add("open");
  filterPopoverEl.setAttribute("aria-hidden", "false");
}

function closeFilterPopover() {
  if (!filterPopoverEl) {
    return;
  }
  filterPopoverEl.classList.remove("open");
  filterPopoverEl.setAttribute("aria-hidden", "true");
}

function setTransferButtonsEnabled(enabled) {
  if (commandBtn) {
    commandBtn.disabled = !enabled;
  }
  if (transferBtn) {
    transferBtn.disabled = !enabled;
  }
}

function setActiveTransferTab(tab) {
  const isUpload = tab === "upload";
  if (uploadTabBtn) {
    uploadTabBtn.classList.toggle("active", isUpload);
  }
  if (downloadTabBtn) {
    downloadTabBtn.classList.toggle("active", !isUpload);
  }
  if (uploadPanelEl) {
    uploadPanelEl.classList.toggle("active", isUpload);
  }
  if (downloadPanelEl) {
    downloadPanelEl.classList.toggle("active", !isUpload);
  }
}

function openTransferModal(tab) {
  if (!selectedHost) {
    showToast("Select a server first.");
    return;
  }
  if (transferHostLabelEl) {
    transferHostLabelEl.textContent = `Server: ${selectedHost}`;
  }
  setActiveTransferTab(tab);
  if (transferModalEl && transferBackdropEl) {
    transferModalEl.classList.add("open");
    transferModalEl.setAttribute("aria-hidden", "false");
    transferBackdropEl.classList.add("open");
  }
}

function closeTransferModal() {
  if (transferModalEl && transferBackdropEl) {
    transferModalEl.classList.remove("open");
    transferModalEl.setAttribute("aria-hidden", "true");
    transferBackdropEl.classList.remove("open");
  }
}

function openCommandModal() {
  if (!selectedHost) {
    showToast("Select a server first.");
    return;
  }
  if (commandHostLabelEl) {
    commandHostLabelEl.textContent = `Server: ${selectedHost}`;
  }
  const session = getCommandSession(selectedHost);
  if (session) {
    setCommandPrompt(selectedHost, session.cwd);
    setCommandOutput(session.buffer || "Output will appear here.");
    session.historyIndex = session.history.length;
  }
  if (commandStatusEl) {
    commandStatusEl.textContent = "Ready.";
  }
  if (commandExitEl) {
    commandExitEl.textContent = "Exit --";
  }
  if (commandModalEl && commandBackdropEl) {
    commandModalEl.classList.add("open");
    commandModalEl.setAttribute("aria-hidden", "false");
    commandBackdropEl.classList.add("open");
  }
  if (commandInputEl) {
    commandInputEl.focus();
  }
  resizeCommandInput();
  scrollTerminalToBottom();
  ensureCommandCwd();
}

function closeCommandModal() {
  if (commandModalEl && commandBackdropEl) {
    commandModalEl.classList.remove("open");
    commandModalEl.setAttribute("aria-hidden", "true");
    commandBackdropEl.classList.remove("open");
  }
}

function resetCommandOutput(message = "Output will appear here.") {
  const session = getCommandSession(selectedHost);
  if (session) {
    session.buffer = "";
    session.historyIndex = session.history.length;
  }
  setCommandOutput(message);
  if (commandStatusEl) {
    commandStatusEl.textContent = "Ready.";
  }
  if (commandExitEl) {
    commandExitEl.textContent = "Exit --";
  }
}

function setCommandBusy(isBusy) {
  commandInProgress = isBusy;
  if (commandRunBtn) {
    commandRunBtn.disabled = isBusy;
  }
  if (commandInputEl) {
    commandInputEl.disabled = isBusy;
  }
}

function resetUploadProgress() {
  if (uploadProgressBar) {
    uploadProgressBar.style.width = "0%";
  }
  if (uploadPercentEl) {
    uploadPercentEl.textContent = "0%";
  }
  if (uploadSpeedEl) {
    uploadSpeedEl.textContent = "--";
  }
  if (uploadStatusEl) {
    uploadStatusEl.textContent = "";
  }
  if (uploadDropText) {
    uploadDropText.textContent = "Drop a file here or click to browse";
  }
}

function resetDownloadProgress() {
  if (downloadProgressBar) {
    downloadProgressBar.style.width = "0%";
  }
  if (downloadPercentEl) {
    downloadPercentEl.textContent = "0%";
  }
  if (downloadSpeedEl) {
    downloadSpeedEl.textContent = "--";
  }
  if (downloadStatusEl) {
    downloadStatusEl.textContent = "";
  }
}

function setUploadBusy(isBusy) {
  uploadInProgress = isBusy;
  if (uploadStartBtn) {
    uploadStartBtn.disabled = isBusy;
  }
  if (uploadFileInput) {
    uploadFileInput.disabled = isBusy;
  }
  if (uploadPathInput) {
    uploadPathInput.disabled = isBusy;
  }
}

function setDownloadBusy(isBusy) {
  downloadInProgress = isBusy;
  if (downloadStartBtn) {
    downloadStartBtn.disabled = isBusy;
  }
  if (downloadPathInput) {
    downloadPathInput.disabled = isBusy;
  }
  if (downloadNameInput) {
    downloadNameInput.disabled = isBusy;
  }
}

function setUploadFile(file) {
  if (!uploadDropText) {
    return;
  }
  if (!file) {
    uploadDropText.textContent = "Drop a file here or click to browse";
    return;
  }
  uploadDropText.textContent = `${file.name} (${formatFileSize(file.size)})`;
}

function clearServers() {
  serverListEl.innerHTML = "";
  serverItems.clear();
}

function applyFilter() {
  const visibleList = hosts.filter((host) => visibleHosts.has(host));
  renderServers(visibleList, hosts.length);
}

function toggleHostVisibility(host, isVisible) {
  if (isVisible) {
    visibleHosts.add(host);
  } else {
    visibleHosts.delete(host);
  }
  manualFilter = true;
  applyFilter();
  renderFilterList(hosts);
}

function renderFilterList(list) {
  if (!filterListEl) {
    return;
  }
  filterListEl.innerHTML = "";
  list.forEach((host) => {
    const item = document.createElement("label");
    item.className = "filter-item";
    const checkbox = document.createElement("input");
    checkbox.type = "checkbox";
    checkbox.checked = visibleHosts.has(host);
    checkbox.addEventListener("change", () => toggleHostVisibility(host, checkbox.checked));
    const text = document.createElement("span");
    text.textContent = host;
    item.appendChild(checkbox);
    item.appendChild(text);
    filterListEl.appendChild(item);
  });
}

function updateServerItemStatus(item, status) {
  item.dataset.status = status;
  item.querySelector(".server-pill").textContent = status;
}

function setServerItemStatus(host, status) {
  if (!host) {
    return;
  }
  serverStatuses.set(host, status);
  const item = serverItems.get(host);
  if (item) {
    updateServerItemStatus(item, status);
  }
}

function showDetailEmpty() {
  detailEmptyEl.style.display = "grid";
  detailBodyEl.style.display = "none";
  detailPaneEl.dataset.status = "idle";
  detailPillEl.textContent = "idle";
  resetDetailMetrics();
  resetProcessPanel();
  setTransferButtonsEnabled(false);
}

function showDetailBody() {
  detailEmptyEl.style.display = "none";
  detailBodyEl.style.display = "grid";
}

function showNoServers() {
  emptyStateEl.style.display = "block";
  detailEmptyEl.style.display = "none";
  detailBodyEl.style.display = "none";
  detailPaneEl.dataset.status = "idle";
  detailPillEl.textContent = "idle";
  detailErrorEl.textContent = "";
  resetDetailMetrics();
  resetProcessPanel();
  setTransferButtonsEnabled(false);
}

function hideNoServers() {
  emptyStateEl.style.display = "none";
}

function resetDetailMetrics() {
  detailMetricCountEl.textContent = "--";
  detailMetricUtilEl.textContent = "--";
  detailMetricMemEl.textContent = "--";
  detailGpuListEl.innerHTML = "";
  detailErrorEl.textContent = "";
  detailHasData = false;
}

function resetProcessPanel() {
  selectedGpuIndex = null;
  processLoadedAt = 0;
  processPanelEl.dataset.status = "idle";
  processTitleEl.textContent = "Processes";
  processSubtitleEl.textContent = "Select a GPU to view processes.";
  processEmptyEl.textContent = "Select a GPU to view processes.";
  processListEl.innerHTML = "";
  processErrorEl.textContent = "";
  processEmptyEl.style.display = "grid";
  processHasData = false;
}

function buildServerItem(host) {
  const node = serverItemTemplate.content.firstElementChild.cloneNode(true);
  node.dataset.host = host;
  node.querySelector(".server-name").textContent = host;
  node.addEventListener("click", () => selectServer(host));
  const status = serverStatuses.get(host) || "idle";
  updateServerItemStatus(node, status);
  serverItems.set(host, node);
  return node;
}

function renderServers(list, totalCount = null) {
  clearServers();
  if (totalCount == null) {
    serverCountEl.textContent = list.length.toString();
  } else {
    serverCountEl.textContent = `${list.length}/${totalCount}`;
  }
  if (!list.length) {
    showNoServers();
    return;
  }
  hideNoServers();
  list.forEach((host) => {
    serverListEl.appendChild(buildServerItem(host));
  });
  if (selectedHost && list.includes(selectedHost)) {
    setActiveServer(selectedHost);
    detailHostEl.textContent = selectedHost;
    showDetailBody();
  } else {
    selectedHost = null;
    showDetailEmpty();
  }
}

function setDetailLoading() {
  detailPaneEl.dataset.status = "loading";
  detailPillEl.textContent = "loading";
  if (!detailHasData) {
    resetDetailMetrics();
  } else {
    detailErrorEl.textContent = "";
  }
  setServerItemStatus(selectedHost, "loading");
}

function renderDetailError(message) {
  detailPaneEl.dataset.status = "error";
  detailPillEl.textContent = "error";
  detailErrorEl.textContent = "";
  setServerItemStatus(selectedHost, "error");
  showToast(message);
}

function renderGpuItem(gpu) {
  const memPct = gpu.mem_total ? Math.round((gpu.mem_used / gpu.mem_total) * 100) : 0;
  const item = document.createElement("div");
  item.className = "gpu-item";
  item.dataset.index = gpu.index;
  item.tabIndex = 0;
  item.setAttribute("role", "button");
  item.addEventListener("click", () => selectGpu(gpu.index));
  item.addEventListener("keydown", (event) => {
    if (event.key === "Enter" || event.key === " ") {
      event.preventDefault();
      selectGpu(gpu.index);
    }
  });
  if (selectedGpuIndex === gpu.index) {
    item.classList.add("active");
  }
  item.innerHTML = `
    <div class="gpu-title">
      <span>GPU ${gpu.index}</span>
      <span class="gpu-name">${gpu.name}</span>
    </div>
    <div class="bar">
      <div class="bar-row">
        <span>Utilization</span>
        <strong>${formatPercent(gpu.util)}</strong>
      </div>
      <div class="meter"><span style="width: ${gpu.util}%;"></span></div>
    </div>
    <div class="bar">
      <div class="bar-row">
        <span>Memory</span>
        <strong>${formatPercent(memPct)}</strong>
      </div>
      <div class="meter"><span style="width: ${memPct}%;"></span></div>
    </div>
    <div class="gpu-meta">
      <span>Temp ${gpu.temp} C</span>
      <span>${formatMiB(gpu.mem_used)} / ${formatMiB(gpu.mem_total)}</span>
    </div>
  `;
  return item;
}

function renderDetailOk(payload) {
  detailPaneEl.dataset.status = "ok";
  detailPillEl.textContent = "ok";
  const summary = payload.summary || {};
  detailMetricCountEl.textContent = summary.count ?? "--";
  detailMetricUtilEl.textContent =
    summary.util_avg != null ? formatPercent(summary.util_avg) : "--";
  if (summary.mem_total) {
    detailMetricMemEl.textContent = `${formatPercent(summary.mem_pct)} used`;
  } else {
    detailMetricMemEl.textContent = "--";
  }

  detailGpuListEl.innerHTML = "";
  const available = new Set();
  payload.gpus.forEach((gpu) => {
    available.add(gpu.index);
    detailGpuListEl.appendChild(renderGpuItem(gpu));
  });
  if (selectedGpuIndex != null && !available.has(selectedGpuIndex)) {
    resetProcessPanel();
  }
  if (!payload.gpus.length) {
    resetProcessPanel();
    processSubtitleEl.textContent = "No GPUs reported on this server.";
  }
  setServerItemStatus(selectedHost, "ok");
  detailHasData = true;
}

function renderProcessItem(process) {
  const item = document.createElement("div");
  item.className = "process-item";
  const pidText = process.pid != null ? process.pid : "--";
  const memText = formatOptionalMiB(process.mem_used);
  let cwdText = process.cwd || "";
  if (!cwdText) {
    cwdText = process.cwd_error ? `unavailable: ${process.cwd_error}` : "unavailable";
  }
  item.innerHTML = `
    <div class="process-title">
      <span class="process-name">${process.name || "unknown"}</span>
      <span class="process-mem">${memText}</span>
    </div>
    <div class="process-meta">
      <span>PID ${pidText}</span>
      <span>GPU ${process.gpu_index ?? "--"}</span>
    </div>
    <div class="process-cwd">${cwdText}</div>
  `;
  return item;
}

function setProcessLoading() {
  processPanelEl.dataset.status = "loading";
  processTitleEl.textContent = `GPU ${selectedGpuIndex} processes`;
  processSubtitleEl.textContent = "Loading processes...";
  processErrorEl.textContent = "";
  if (!processHasData) {
    processListEl.innerHTML = "";
    processEmptyEl.style.display = "none";
  }
}

function renderProcessError(message) {
  processPanelEl.dataset.status = "error";
  processTitleEl.textContent =
    selectedGpuIndex != null ? `GPU ${selectedGpuIndex} processes` : "Processes";
  if (processHasData) {
    processSubtitleEl.textContent = "Failed to refresh. Showing last data.";
  } else {
    processSubtitleEl.textContent = "Failed to load processes.";
  }
  processErrorEl.textContent = "";
  if (!processHasData) {
    processListEl.innerHTML = "";
    processEmptyEl.style.display = "none";
  }
  showToast(message);
}

function renderProcessList(processes) {
  processPanelEl.dataset.status = "ok";
  processTitleEl.textContent = `GPU ${selectedGpuIndex} processes`;
  processErrorEl.textContent = "";
  processListEl.innerHTML = "";
  if (!processes.length) {
    processSubtitleEl.textContent = "0 processes detected.";
    processEmptyEl.textContent = "No running processes on this GPU.";
    processEmptyEl.style.display = "grid";
    processHasData = true;
    return;
  }
  processSubtitleEl.textContent = `${processes.length} process${processes.length > 1 ? "es" : ""}`;
  processEmptyEl.style.display = "none";
  processes.forEach((process) => processListEl.appendChild(renderProcessItem(process)));
  processHasData = true;
}

function setActiveGpu(index) {
  detailGpuListEl.querySelectorAll(".gpu-item").forEach((item) => {
    const itemIndex = Number(item.dataset.index);
    item.classList.toggle("active", itemIndex === index);
  });
}

async function loadProcessesForSelectedGpu(options = {}) {
  if (!selectedHost || selectedGpuIndex == null) {
    return false;
  }
  const now = Date.now();
  if (!options.force && now - processLoadedAt < REFRESH_MS) {
    return true;
  }
  setProcessLoading();
  try {
    const response = await fetch(
      `/api/gpu-processes?host=${encodeURIComponent(selectedHost)}&index=${selectedGpuIndex}`
    );
    const raw = await response.text();
    let data = null;
    if (raw) {
      try {
        data = JSON.parse(raw);
      } catch (error) {
        data = null;
      }
    }
    if (!response.ok) {
      const message = data?.error || raw || "Failed to refresh GPU processes";
      throw new Error(message);
    }
    if (!data) {
      throw new Error("Invalid process response");
    }
    if (!data.ok) {
      renderProcessError(data.error || "unknown error");
      processLoadedAt = Date.now();
      setLastUpdated();
      return false;
    }
    renderProcessList(data.processes || []);
    processLoadedAt = Date.now();
    setLastUpdated();
    return true;
  } catch (error) {
    renderProcessError(error.message);
    processLoadedAt = Date.now();
    setLastUpdated();
    return false;
  }
}

function setActiveServer(host) {
  serverItems.forEach((item, key) => {
    item.classList.toggle("active", key === host);
  });
}

async function loadStatusForSelected(options = {}) {
  if (!selectedHost) {
    return false;
  }
  const now = Date.now();
  if (!options.force && now - selectedLoadedAt < REFRESH_MS) {
    return true;
  }
  setDetailLoading();
  try {
    const response = await fetch(`/api/status?host=${encodeURIComponent(selectedHost)}`);
    if (!response.ok) {
      throw new Error("Failed to refresh status");
    }
    const result = await response.json();
    if (!result.ok) {
      renderDetailError(result.error || "unknown error");
      selectedLoadedAt = Date.now();
      setLastUpdated();
      return false;
    } else {
      renderDetailOk(result);
    }
    selectedLoadedAt = Date.now();
    setLastUpdated();
    return true;
  } catch (error) {
    renderDetailError(error.message);
    selectedLoadedAt = Date.now();
    setLastUpdated();
    return false;
  }
}

function selectGpu(index) {
  if (selectedGpuIndex === index) {
    setStatus(`Refreshing GPU ${index} processes...`);
    loadProcessesForSelectedGpu({ force: true })
      .then((ok) => setStatus(ok ? `Loaded GPU ${index} processes` : `Failed to load GPU ${index}`));
    return;
  }
  selectedGpuIndex = index;
  processLoadedAt = 0;
  processHasData = false;
  setActiveGpu(index);
  setStatus(`Loading GPU ${index} processes...`);
  loadProcessesForSelectedGpu({ force: true })
    .then((ok) => setStatus(ok ? `Loaded GPU ${index} processes` : `Failed to load GPU ${index}`));
}

function selectServer(host) {
  if (selectedHost === host) {
    return;
  }
  selectedHost = host;
  selectedLoadedAt = 0;
  resetDetailMetrics();
  resetProcessPanel();
  setActiveServer(host);
  setTransferButtonsEnabled(true);
  detailHostEl.textContent = host;
  showDetailBody();
  setStatus(`Loading ${host}...`);
  loadStatusForSelected({ force: true })
    .then((ok) => setStatus(ok ? `Loaded ${host}` : `Failed to load ${host}`));
}

function startUpload() {
  if (uploadInProgress) {
    return;
  }
  if (!selectedHost) {
    showToast("Select a server first.");
    return;
  }
  const file = uploadFileInput?.files?.[0];
  const remotePath = uploadPathInput?.value?.trim();
  if (!file) {
    showToast("Choose a local file first.");
    return;
  }
  if (!remotePath) {
    showToast("Enter a remote path.");
    return;
  }
  resetUploadProgress();
  if (uploadStatusEl) {
    uploadStatusEl.textContent = "Uploading...";
  }
  setUploadBusy(true);
  const url = `/api/upload?host=${encodeURIComponent(selectedHost)}&path=${encodeURIComponent(
    remotePath
  )}&name=${encodeURIComponent(file.name)}`;
  const xhr = new XMLHttpRequest();
  const startTime = performance.now();
  xhr.open("POST", url, true);
  xhr.setRequestHeader("Content-Type", "application/octet-stream");
  xhr.upload.onprogress = (event) => {
    if (!uploadProgressBar || !uploadPercentEl || !uploadSpeedEl) {
      return;
    }
    if (event.lengthComputable) {
      const percent = Math.round((event.loaded / event.total) * 100);
      uploadProgressBar.style.width = `${percent}%`;
      uploadPercentEl.textContent = `${percent}%`;
    } else {
      uploadPercentEl.textContent = "--";
    }
    const elapsed = (performance.now() - startTime) / 1000;
    const speed = elapsed > 0 ? event.loaded / elapsed : 0;
    uploadSpeedEl.textContent = formatBytesPerSecond(speed);
  };
  xhr.onload = () => {
    setUploadBusy(false);
    const ok = xhr.status >= 200 && xhr.status < 300;
    if (uploadProgressBar) {
      uploadProgressBar.style.width = "100%";
    }
    if (uploadPercentEl) {
      uploadPercentEl.textContent = ok ? "100%" : "0%";
    }
    if (uploadStatusEl) {
      uploadStatusEl.textContent = ok ? "Upload completed." : "Upload failed.";
    }
    if (!ok) {
      let message = xhr.responseText;
      if (message) {
        try {
          const parsed = JSON.parse(message);
          message = parsed.error || message;
        } catch (error) {
          message = xhr.responseText;
        }
      }
      showToast(message || "Upload failed.");
    }
  };
  xhr.onerror = () => {
    setUploadBusy(false);
    if (uploadStatusEl) {
      uploadStatusEl.textContent = "Upload failed.";
    }
    showToast("Upload failed.");
  };
  xhr.send(file);
}

function startDownload() {
  if (downloadInProgress) {
    return;
  }
  if (!selectedHost) {
    showToast("Select a server first.");
    return;
  }
  const remotePath = downloadPathInput?.value?.trim();
  if (!remotePath) {
    showToast("Enter a remote path.");
    return;
  }
  let localName = downloadNameInput?.value?.trim();
  if (!localName) {
    localName = getBaseName(remotePath) || "download.bin";
  }
  resetDownloadProgress();
  if (downloadStatusEl) {
    downloadStatusEl.textContent = "Downloading...";
  }
  setDownloadBusy(true);
  const url = `/api/download?host=${encodeURIComponent(selectedHost)}&path=${encodeURIComponent(
    remotePath
  )}`;
  const xhr = new XMLHttpRequest();
  const startTime = performance.now();
  xhr.open("GET", url, true);
  xhr.responseType = "blob";
  xhr.onprogress = (event) => {
    if (!downloadProgressBar || !downloadPercentEl || !downloadSpeedEl) {
      return;
    }
    if (event.lengthComputable) {
      const percent = Math.round((event.loaded / event.total) * 100);
      downloadProgressBar.style.width = `${percent}%`;
      downloadPercentEl.textContent = `${percent}%`;
    } else {
      downloadPercentEl.textContent = "--";
    }
    const elapsed = (performance.now() - startTime) / 1000;
    const speed = elapsed > 0 ? event.loaded / elapsed : 0;
    downloadSpeedEl.textContent = formatBytesPerSecond(speed);
  };
  xhr.onload = () => {
    setDownloadBusy(false);
    if (xhr.status >= 200 && xhr.status < 300) {
      if (downloadProgressBar) {
        downloadProgressBar.style.width = "100%";
      }
      if (downloadPercentEl) {
        downloadPercentEl.textContent = "100%";
      }
      if (downloadStatusEl) {
        downloadStatusEl.textContent = "Download completed.";
      }
      const blob = xhr.response;
      const link = document.createElement("a");
      link.href = URL.createObjectURL(blob);
      link.download = localName;
      document.body.appendChild(link);
      link.click();
      link.remove();
      URL.revokeObjectURL(link.href);
    } else {
      if (downloadStatusEl) {
        downloadStatusEl.textContent = "Download failed.";
      }
      if (xhr.response && typeof xhr.response.text === "function") {
        xhr.response.text().then((message) => {
          let text = message;
          if (text) {
            try {
              const parsed = JSON.parse(text);
              text = parsed.error || message;
            } catch (error) {
              text = message;
            }
          }
          showToast(text || "Download failed.");
        });
      } else {
        showToast(xhr.responseText || "Download failed.");
      }
    }
  };
  xhr.onerror = () => {
    setDownloadBusy(false);
    if (downloadStatusEl) {
      downloadStatusEl.textContent = "Download failed.";
    }
    showToast("Download failed.");
  };
  xhr.send();
}

async function fetchCommandData(host, command, cwd) {
  const response = await fetch("/api/command", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ host, command, cwd }),
  });
  const raw = await response.text();
  let data = null;
  if (raw) {
    try {
      data = JSON.parse(raw);
    } catch (error) {
      data = null;
    }
  }
  if (!response.ok || !data) {
    const message = data?.error || raw || "Command failed.";
    throw new Error(message);
  }
  return data;
}

async function ensureCommandCwd() {
  if (!selectedHost) {
    return;
  }
  if (commandInProgress) {
    return;
  }
  const session = getCommandSession(selectedHost);
  if (!session || session.cwd) {
    return;
  }
  const requestHost = selectedHost;
  try {
    const data = await fetchCommandData(selectedHost, "pwd", session.cwd);
    if (selectedHost !== requestHost) {
      return;
    }
    if (data.cwd && !session.cwd) {
      session.cwd = data.cwd;
      setCommandPrompt(selectedHost, session.cwd);
    }
  } catch (error) {
    showToast(error.message || "Failed to fetch working directory.");
  }
}

async function handleTabCompletion() {
  if (commandInProgress || !selectedHost || !commandInputEl) {
    return;
  }
  const session = getCommandSession(selectedHost);
  if (session && !session.cwd) {
    await ensureCommandCwd();
  }
  const value = commandInputEl.value;
  const cursor = commandInputEl.selectionStart ?? value.length;
  const left = value.slice(0, cursor);
  const match = left.match(/(^|\s)([^\s]*)$/);
  if (!match) {
    return;
  }
  const token = match[2] || "";
  if (!token) {
    return;
  }
  const tokenStart = cursor - token.length;
  const beforeToken = value.slice(0, tokenStart);
  const isFirstToken = !/\S/.test(beforeToken);
  const mode = isFirstToken && !token.includes("/") ? "command" : "file";
  try {
    const response = await fetch("/api/command-complete", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        host: selectedHost,
        cwd: session?.cwd || "",
        prefix: token,
        mode,
      }),
    });
    if (!response.ok) {
      throw new Error("Completion failed.");
    }
    const data = await response.json();
    if (!data.ok) {
      throw new Error(data.error || "Completion failed.");
    }
    const matches = data.matches || [];
    if (!matches.length) {
      return;
    }
    if (matches.length === 1) {
      const completion = matches[0];
      commandInputEl.value =
        value.slice(0, tokenStart) + completion + value.slice(cursor);
      const caret = tokenStart + completion.length;
      commandInputEl.setSelectionRange(caret, caret);
      resizeCommandInput();
      return;
    }
    const common = longestCommonPrefix(matches);
    if (common && common.length > token.length) {
      commandInputEl.value =
        value.slice(0, tokenStart) + common + value.slice(cursor);
      const caret = tokenStart + common.length;
      commandInputEl.setSelectionRange(caret, caret);
      resizeCommandInput();
      return;
    }
    if (session) {
      appendCommandOutput(session, matches.join("  "));
    }
  } catch (error) {
    showToast(error.message || "Completion failed.");
  }
}

async function runCommand(commandOverride = null) {
  if (commandInProgress) {
    return;
  }
  if (!selectedHost) {
    showToast("Select a server first.");
    return;
  }
  const session = getCommandSession(selectedHost);
  const overrideValue = typeof commandOverride === "string" ? commandOverride : null;
  const inputValue = commandInputEl?.value ?? "";
  const commandValue = (overrideValue ?? inputValue).trim();
  if (!commandValue) {
    showToast("Enter a command.");
    return;
  }
  if (commandInputEl) {
    commandInputEl.value = "";
    resizeCommandInput();
  }
  setCommandBusy(true);
  if (commandStatusEl) {
    commandStatusEl.textContent = "Running...";
  }
  if (commandExitEl) {
    commandExitEl.textContent = "Exit --";
  }
  if (session) {
    const prompt = formatPrompt(selectedHost, session.cwd);
    appendCommandOutput(session, `${prompt} ${commandValue}`);
    session.history.push(commandValue);
    session.historyIndex = session.history.length;
  }
  try {
    const data = await fetchCommandData(selectedHost, commandValue, session?.cwd || "");
    if (data.cwd && session) {
      session.cwd = data.cwd;
      setCommandPrompt(selectedHost, session.cwd);
    }
    const parts = [];
    if (data.stdout) {
      parts.push(data.stdout);
    }
    if (data.stderr) {
      parts.push(`[stderr]\n${data.stderr}`);
    }
    const outputText = parts.join("\n\n");
    if (outputText && session) {
      appendCommandOutput(session, outputText);
    } else if (!outputText && session) {
      appendCommandOutput(session, "");
    }
    if (commandExitEl) {
      const exitCode = data.exit_code != null ? data.exit_code : "--";
      commandExitEl.textContent = `Exit ${exitCode}`;
    }
    if (commandStatusEl) {
      commandStatusEl.textContent = data.ok ? "Completed." : "Completed with errors.";
    }
    if (!data.ok) {
      showToast(data.error || `Command failed (exit ${data.exit_code ?? "--"})`);
    }
  } catch (error) {
    if (commandStatusEl) {
      commandStatusEl.textContent = "Failed.";
    }
    if (commandExitEl) {
      commandExitEl.textContent = "Exit --";
    }
    showToast(error.message || "Command failed.");
  } finally {
    setCommandBusy(false);
    if (commandInputEl) {
      commandInputEl.focus();
    }
  }
}

async function loadServers() {
  const response = await fetch("/api/servers");
  if (!response.ok) {
    throw new Error("Failed to load servers");
  }
  const data = await response.json();
  configPathEl.textContent = data.config || "--";
  return data.hosts || [];
}

async function loadStartupStatus() {
  if (!startupPanelEl || !startupToggleEl || !startupStatusEl) {
    return;
  }
  try {
    const response = await fetch("/api/startup");
    const raw = await response.text();
    const data = raw ? JSON.parse(raw) : {};
    if (!data.supported) {
      setStartupVisible(false);
      return;
    }
    setStartupVisible(true);
    if (!data.ok) {
      setStartupState(false, "Unavailable");
      setStartupBusy(true);
      if (data.error) {
        showToast(data.error);
      }
      return;
    }
    setStartupState(!!data.enabled, data.enabled ? "Enabled" : "Disabled");
    setStartupBusy(false);
  } catch (error) {
    setStartupVisible(true);
    setStartupState(false, "Unavailable");
    setStartupBusy(true);
    showToast("Failed to load startup status.");
  }
}

async function refreshAll() {
  try {
    hosts = await loadServers();
    if (!manualFilter) {
      visibleHosts = new Set(hosts);
    } else {
      visibleHosts = new Set(hosts.filter((host) => visibleHosts.has(host)));
    }
    renderFilterList(hosts);
    if (!hosts.length) {
      renderServers([]);
      setStatus("No hosts found in SSH config");
      return;
    }
    applyFilter();
    if (selectedHost) {
      const ok = await loadStatusForSelected({ force: true });
      let processesOk = true;
      if (selectedGpuIndex != null) {
        processesOk = await loadProcessesForSelectedGpu({ force: true });
      }
      if (ok && processesOk) {
        setStatus(`Updated ${selectedHost}`);
      } else if (!ok) {
        setStatus(`Failed to update ${selectedHost}`);
      } else {
        setStatus(`Updated ${selectedHost}, but GPU processes failed`);
      }
    } else {
      setStatus("Select a server to view details");
    }
  } catch (error) {
    setStatus(error.message);
    showToast(error.message);
    renderServers([]);
  }
}

async function refreshSelected(force = false) {
  if (!selectedHost) {
    return;
  }
  setStatus(`Refreshing ${selectedHost}...`);
  const ok = await loadStatusForSelected({ force });
  let processesOk = true;
  if (selectedGpuIndex != null) {
    processesOk = await loadProcessesForSelectedGpu({ force });
  }
  if (ok && processesOk) {
    setStatus(`Updated ${selectedHost}`);
  } else if (!ok) {
    setStatus(`Failed to update ${selectedHost}`);
  } else {
    setStatus(`Updated ${selectedHost}, but GPU processes failed`);
  }
}

function scheduleRefresh() {
  if (refreshTimer) {
    clearInterval(refreshTimer);
  }
  refreshTimer = setInterval(() => refreshSelected(false), REFRESH_MS);
}

refreshBtn.addEventListener("click", refreshAll);
if (selectAllBtn) {
  selectAllBtn.addEventListener("click", () => {
    visibleHosts = new Set(hosts);
    manualFilter = true;
    renderFilterList(hosts);
    applyFilter();
  });
}
if (clearAllBtn) {
  clearAllBtn.addEventListener("click", () => {
    visibleHosts = new Set();
    manualFilter = true;
    renderFilterList(hosts);
    applyFilter();
  });
}
if (filterToggleBtn) {
  filterToggleBtn.addEventListener("click", () => {
    if (!filterPopoverEl) {
      return;
    }
    if (filterPopoverEl.classList.contains("open")) {
      closeFilterPopover();
    } else {
      openFilterPopover();
    }
  });
}
document.addEventListener("keydown", (event) => {
  if (event.key === "Escape") {
    closeFilterPopover();
    closeTransferModal();
    closeCommandModal();
  }
});
document.addEventListener("click", (event) => {
  if (!filterPopoverEl || !filterToggleBtn) {
    return;
  }
  if (!filterPopoverEl.classList.contains("open")) {
    return;
  }
  const target = event.target;
  if (filterPopoverEl.contains(target) || filterToggleBtn.contains(target)) {
    return;
  }
  closeFilterPopover();
});
if (transferBtn) {
  transferBtn.addEventListener("click", () => {
    resetUploadProgress();
    resetDownloadProgress();
    openTransferModal("upload");
  });
}
if (commandBtn) {
  commandBtn.addEventListener("click", () => {
    openCommandModal();
  });
}
if (transferCloseBtn) {
  transferCloseBtn.addEventListener("click", closeTransferModal);
}
if (transferBackdropEl) {
  transferBackdropEl.addEventListener("click", closeTransferModal);
}
if (commandCloseBtn) {
  commandCloseBtn.addEventListener("click", closeCommandModal);
}
if (commandBackdropEl) {
  commandBackdropEl.addEventListener("click", closeCommandModal);
}
if (uploadTabBtn) {
  uploadTabBtn.addEventListener("click", () => setActiveTransferTab("upload"));
}
if (downloadTabBtn) {
  downloadTabBtn.addEventListener("click", () => setActiveTransferTab("download"));
}
if (uploadStartBtn) {
  uploadStartBtn.addEventListener("click", startUpload);
}
if (downloadStartBtn) {
  downloadStartBtn.addEventListener("click", startDownload);
}
if (commandRunBtn) {
  commandRunBtn.addEventListener("click", () => runCommand());
}
if (commandClearBtn) {
  commandClearBtn.addEventListener("click", () => resetCommandOutput());
}
if (commandInputEl) {
  commandInputEl.addEventListener("input", () => {
    resizeCommandInput();
  });
  commandInputEl.addEventListener("keydown", (event) => {
    const session = getCommandSession(selectedHost);
    if (event.key === "Enter" && !event.shiftKey) {
      event.preventDefault();
      runCommand();
      return;
    }
    if (event.key === "Tab") {
      event.preventDefault();
      handleTabCompletion();
      return;
    }
    if (event.key === "ArrowUp" && session?.history.length) {
      if (commandInputEl.selectionStart > 0) {
        return;
      }
      event.preventDefault();
      if (session.historyIndex <= 0) {
        session.historyIndex = 0;
      } else if (session.historyIndex > 0) {
        session.historyIndex -= 1;
      }
      commandInputEl.value = session.history[session.historyIndex] || "";
      resizeCommandInput();
      return;
    }
    if (event.key === "ArrowDown" && session?.history.length) {
      if (commandInputEl.selectionStart < commandInputEl.value.length) {
        return;
      }
      event.preventDefault();
      if (session.historyIndex >= session.history.length - 1) {
        session.historyIndex = session.history.length;
        commandInputEl.value = "";
      } else {
        session.historyIndex += 1;
        commandInputEl.value = session.history[session.historyIndex] || "";
      }
      resizeCommandInput();
      return;
    }
    if ((event.ctrlKey || event.metaKey) && event.key.toLowerCase() === "l") {
      event.preventDefault();
      resetCommandOutput("Output cleared.");
    }
  });
}
if (downloadPathInput && downloadNameInput) {
  downloadPathInput.addEventListener("change", () => {
    if (!downloadNameInput.value.trim()) {
      downloadNameInput.value = getBaseName(downloadPathInput.value.trim());
    }
  });
}
if (uploadFileInput) {
  uploadFileInput.addEventListener("change", () => {
    const file = uploadFileInput.files?.[0] || null;
    setUploadFile(file);
  });
}
if (uploadDropzone && uploadFileInput) {
  uploadDropzone.addEventListener("click", () => {
    uploadFileInput.click();
  });
  uploadDropzone.addEventListener("keydown", (event) => {
    if (event.key === "Enter" || event.key === " ") {
      event.preventDefault();
      uploadFileInput.click();
    }
  });
  const stopEvent = (event) => {
    event.preventDefault();
    event.stopPropagation();
  };
  ["dragenter", "dragover"].forEach((name) => {
    uploadDropzone.addEventListener(name, (event) => {
      stopEvent(event);
      uploadDropzone.classList.add("dragover");
    });
  });
  ["dragleave", "drop"].forEach((name) => {
    uploadDropzone.addEventListener(name, (event) => {
      stopEvent(event);
      uploadDropzone.classList.remove("dragover");
    });
  });
  uploadDropzone.addEventListener("drop", (event) => {
    const file = event.dataTransfer?.files?.[0];
    if (!file) {
      return;
    }
    const data = new DataTransfer();
    data.items.add(file);
    uploadFileInput.files = data.files;
    setUploadFile(file);
  });
}

if (startupToggleEl) {
  startupToggleEl.addEventListener("change", async () => {
    if (startupUpdating) {
      return;
    }
    const desired = startupToggleEl.checked;
    setStartupBusy(true);
    try {
      const response = await fetch("/api/startup", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ enabled: desired }),
      });
      const raw = await response.text();
      const data = raw ? JSON.parse(raw) : {};
      if (!response.ok || !data.ok) {
        throw new Error(data.error || "Startup update failed.");
      }
      setStartupState(!!data.enabled, data.enabled ? "Enabled" : "Disabled");
    } catch (error) {
      setStartupState(!desired, !desired ? "Disabled" : "Enabled");
      showToast(error.message || "Startup update failed.");
    } finally {
      setStartupBusy(false);
    }
  });
}

setTransferButtonsEnabled(false);
loadStartupStatus();
refreshAll();
scheduleRefresh();
