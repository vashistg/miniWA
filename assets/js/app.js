import {Socket} from "phoenix"

// ─── State ────────────────────────────────────────────────────────────────────
let socket         = null
let channel        = null
let myUserId       = null
let activeConv     = null        // id of open conversation (user_id or group_id)
let activeConvType = null        // "user" | "group"
const tickMap      = {}
const sendTimeMap  = {}         // clientId → Date.now() at send, for latency measurement
let   pendingMedia = null       // {url, mediaType, name} — set after a successful upload
const allUsers     = new Map()   // user_id → { online: bool }
const myGroups     = new Map()   // group_id → { name }
const unreadCounts = new Map()   // conv_id → number
const pendingRead  = []          // {message_id, from}

// ─── DOM Helpers ──────────────────────────────────────────────────────────────
const $ = id => document.getElementById(id)

function log(text, type = "info") {
  const el = document.createElement("div")
  el.className = `log-line log-${type}`
  const t = new Date().toLocaleTimeString("en-GB", {hour12: false})
  el.textContent = `${t}  ${text}`
  $("event-log").appendChild(el)
  $("event-log").scrollTop = $("event-log").scrollHeight
}

function escHtml(s) {
  return String(s).replace(/&/g,"&amp;").replace(/</g,"&lt;").replace(/>/g,"&gt;")
}

// ─── Log toggle ───────────────────────────────────────────────────────────────
window.toggleLog = () => $("log-panel").classList.toggle("collapsed")

// ─── Local storage ────────────────────────────────────────────────────────────
function convKey(id) { return `miniwa_msgs_${myUserId}_${id}` }

function loadMessages(convId) {
  try { return JSON.parse(localStorage.getItem(convKey(convId)) || "[]") }
  catch (_) { return [] }
}

function saveMessage(convId, msg) {
  const msgs = loadMessages(convId)
  if (msg.messageId && msgs.some(m => m.messageId === msg.messageId)) return
  msgs.push(msg)
  localStorage.setItem(convKey(convId), JSON.stringify(msgs))
}

function updateStoredMessageId(clientId, messageId, tick) {
  for (let i = 0; i < localStorage.length; i++) {
    const key = localStorage.key(i)
    if (!key || !key.startsWith(`miniwa_msgs_${myUserId}_`)) continue
    try {
      const msgs    = JSON.parse(localStorage.getItem(key) || "[]")
      const updated = msgs.map(m => m.clientId === clientId ? {...m, messageId, tick} : m)
      if (JSON.stringify(msgs) !== JSON.stringify(updated))
        localStorage.setItem(key, JSON.stringify(updated))
    } catch (_) {}
  }
}

// ─── Connect ──────────────────────────────────────────────────────────────────
window.connect = function () {
  const username = $("username-input").value.trim()
  if (!username) { alert("Enter a username first"); return }

  myUserId = username
  log(`Connecting as "${username}"…`, "system")

  socket = new Socket("/socket", {params: {user_id: username}})
  socket.connect()
  socket.onOpen(()  => log("WebSocket open ✓", "success"))
  socket.onError(() => log("WebSocket error", "error"))
  socket.onClose(() => log("WebSocket closed", "system"))

  channel = socket.channel(`room:${username}`, {})
  window.__mwa = {
    channel:   () => channel,
    allUsers:  () => allUsers,
    myGroups:  () => myGroups,
    // burst(to, n) — send n numbered messages through the normal send path
    burst(to, n = 20) {
      openConversation(to, "user")
      let i = 0
      const tick = setInterval(() => {
        i++
        $("msg-input").value = `msg_${String(i).padStart(2, "0")}`
        window.sendMessage()
        if (i >= n) clearInterval(tick)
      }, 50)
    }
  }

  // ── Server → Client ─────────────────────────────────────────────────────

  channel.on("msg", ({from, content, message_id, conversation_id,
                      client_sent_at, kafka_published_at_ms, delivered_at_ms,
                      media_url, media_type}) => {
    const convId  = conversation_id || from
    const nowMs   = Date.now()
    const parts   = []
    if (kafka_published_at_ms && delivered_at_ms)
      parts.push(`consumer=${delivered_at_ms - kafka_published_at_ms}ms`)
    if (client_sent_at && delivered_at_ms)
      parts.push(`server=${delivered_at_ms - client_sent_at}ms`)
    if (client_sent_at)
      parts.push(`e2e≈${nowMs - client_sent_at}ms`)
    const latency = parts.length ? `  [${parts.join("  ")}]` : ""
    log(`← msg  from=${from}  conv=${convId}${latency}`, "recv")

    saveMessage(convId, {direction: "incoming", from, content, messageId: message_id, convId, mediaUrl: media_url, mediaType: media_type})

    if (activeConv === convId) {
      renderIncomingMessage(from, content, message_id, media_url, media_type)
    } else {
      unreadCounts.set(convId, (unreadCounts.get(convId) || 0) + 1)
      renderSidebar()
    }

    log(`→ delivered  id=${message_id}  from=${from}`, "send")
    channel.push("delivered", {message_id, from})

    if (document.hasFocus() && activeConv === convId) {
      setTimeout(() => sendRead(message_id, from), 800)
    } else {
      pendingRead.push({message_id, from})
    }
  })

  channel.on("tick1", ({client_id, message_id, kafka_published_at_ms}) => {
    const sendTime = sendTimeMap[client_id]
    const wsKafkaMs = sendTime ? Date.now() - sendTime : null
    const latency = wsKafkaMs !== null ? `  [ws+kafka=${wsKafkaMs}ms]` : ""
    log(`← tick1 ✓  ${client_id} → ${message_id}${latency}`, "tick")
    if (tickMap[client_id]) {
      tickMap[message_id] = tickMap[client_id]
      setTick(client_id, "✓", "tick1")
      updateStoredMessageId(client_id, message_id, "tick1")
    }
  })

  channel.on("tick2", ({message_id}) => {
    log(`← tick2 ✓✓  ${message_id}`, "tick")
    setTick(message_id, "✓✓", "tick2")
  })

  channel.on("tick3", ({message_id}) => {
    log(`← tick3 🔵  ${message_id}`, "tick")
    setTick(message_id, "✓✓", "tick3")
  })

  channel.on("presence_join", ({user_id}) => {
    if (user_id === myUserId) return
    allUsers.set(user_id, {online: true})
    log(`◆ ${user_id} ONLINE`, "system")
    renderSidebar()
  })

  channel.on("presence_leave", ({user_id}) => {
    if (allUsers.has(user_id)) allUsers.set(user_id, {online: false})
    log(`◆ ${user_id} OFFLINE`, "system")
    renderSidebar()
  })

  channel.on("group_invite", ({group_id, name, invited_by}) => {
    myGroups.set(group_id, {name})
    log(`◆ You were added to group "${name}" by ${invited_by}`, "system")
    renderSidebar()
  })

  channel.on("typing", ({from, conv_id}) => {
    showTyping(from, conv_id)
  })

  channel.on("removed_from_group", ({group_id}) => {
    const name = myGroups.get(group_id)?.name || group_id
    myGroups.delete(group_id)
    log(`◆ You were removed from group "${name}"`, "system")
    if (activeConv === group_id) {
      activeConv     = null
      activeConvType = null
      $("messages").innerHTML = `<div class="no-conversation">You were removed from this group.</div>`
      $("chat-with").textContent = "Select a conversation"
      $("add-member-btn").style.display = "none"
    }
    renderSidebar()
  })

  channel.join()
    .receive("ok", (resp) => {
      log(`Joined room:${username} ✓`, "success")
      $("login-screen").style.display = "none"
      $("chat-screen").style.display  = "flex"
      $("display-name").textContent   = username

      if (resp.users) {
        resp.users.forEach(({user_id, online}) => {
          if (user_id !== myUserId) allUsers.set(user_id, {online})
        })
      }
      if (resp.groups) {
        resp.groups.forEach(({group_id, name}) => myGroups.set(group_id, {name}))
        log(`Loaded ${resp.groups.length} group(s) from ScyllaDB`, "system")
      }
      renderSidebar()
    })
    .receive("error", r  => log(`Join failed: ${JSON.stringify(r)}`, "error"))
    .receive("timeout", () => log("Join timed out", "error"))
}

// ─── Sidebar ──────────────────────────────────────────────────────────────────
function renderSidebar() {
  const list = $("conversation-list")
  list.innerHTML = ""

  // Groups section
  myGroups.forEach(({name}, groupId) => {
    const unread   = unreadCounts.get(groupId) || 0
    const isActive = activeConv === groupId
    const item     = document.createElement("div")
    item.className = `conv-item${isActive ? " active" : ""}`
    item.innerHTML = `
      <div class="conv-avatar group-avatar">#</div>
      <div class="conv-info">
        <div class="conv-name">${escHtml(name)}</div>
        <div class="conv-status online"><span class="status-dot"></span>Group</div>
      </div>
      ${unread > 0 ? `<div class="unread-badge">${unread}</div>` : ""}
    `
    item.onclick = () => openConversation(groupId, "group")
    list.appendChild(item)
  })

  // Users section
  const sorted = [...allUsers.entries()].sort(([a, av], [b, bv]) => {
    if (av.online !== bv.online) return bv.online ? 1 : -1
    return a.localeCompare(b)
  })

  sorted.forEach(([uid, {online}]) => {
    const unread   = unreadCounts.get(uid) || 0
    const isActive = activeConv === uid
    const item     = document.createElement("div")
    item.className = `conv-item${isActive ? " active" : ""}`
    item.innerHTML = `
      <div class="conv-avatar">${escHtml(uid[0].toUpperCase())}</div>
      <div class="conv-info">
        <div class="conv-name">${escHtml(uid)}</div>
        <div class="conv-status ${online ? "online" : "offline"}">
          <span class="status-dot"></span>${online ? "Online" : "Offline"}
        </div>
      </div>
      ${unread > 0 ? `<div class="unread-badge">${unread}</div>` : ""}
    `
    item.onclick = () => openConversation(uid, "user")
    list.appendChild(item)
  })

  if (myGroups.size === 0 && allUsers.size === 0) {
    list.innerHTML = `<div class="sidebar-empty">No chats yet</div>`
  }
}

// ─── Open conversation ────────────────────────────────────────────────────────
function openConversation(id, type) {
  activeConv     = id
  activeConvType = type
  unreadCounts.set(id, 0)

  $("messages").innerHTML = ""
  $("chat-with").textContent = type === "group"
    ? `# ${myGroups.get(id)?.name || id}`
    : id

  // Show "Add member" button only for groups
  $("add-member-btn").style.display = type === "group" ? "inline-block" : "none"
  clearTyping()

  const msgs = loadMessages(id)
  if (msgs.length === 0) {
    $("messages").innerHTML = `<div class="no-conversation">No messages yet — say hello!</div>`
  } else {
    msgs.forEach(m => {
      if (m.direction === "outgoing") renderOutgoingMessage(m.to || id, m.content, m.clientId, m.messageId, m.tick, m.mediaUrl, m.mediaType)
      else renderIncomingMessage(m.from, m.content, m.messageId, m.mediaUrl, m.mediaType)
    })
  }

  renderSidebar()
  $("msg-input").focus()

  // Flush any pending read receipts for this conversation
  const flush = pendingRead.filter(r => r.from === id || r.convId === id)
  flush.forEach(r => {
    pendingRead.splice(pendingRead.indexOf(r), 1)
    sendRead(r.message_id, r.from)
  })
}

// ─── Send ─────────────────────────────────────────────────────────────────────
window.sendMessage = function () {
  const content = $("msg-input").value.trim()
  if (!content && !pendingMedia) return
  if (!activeConv) { log("Select a conversation first", "error"); return }

  const clientId     = `${Date.now()}-${Math.random().toString(36).slice(2, 7)}`
  const clientSentAt = Date.now()
  sendTimeMap[clientId] = clientSentAt

  const mediaUrl  = pendingMedia?.url  || null
  const mediaType = pendingMedia?.mediaType || null

  saveMessage(activeConv, {direction: "outgoing", to: activeConv, content, clientId, messageId: null, tick: "pending", mediaUrl, mediaType})
  renderOutgoingMessage(activeConv, content, clientId, null, "pending", mediaUrl, mediaType)

  const extra = mediaUrl ? {media_url: mediaUrl, media_type: mediaType} : {}

  if (activeConvType === "group") {
    log(`→ send_group_msg  group=${activeConv}${mediaType ? "  ["+mediaType+"]" : ""}`, "send")
    channel.push("send_group_msg", {group_id: activeConv, content, client_id: clientId, client_sent_at: clientSentAt, ...extra})
  } else {
    log(`→ send_msg  to=${activeConv}${mediaType ? "  ["+mediaType+"]" : ""}`, "send")
    channel.push("send_msg", {to: activeConv, content, client_id: clientId, client_sent_at: clientSentAt, ...extra})
  }

  $("msg-input").value = ""
  clearMediaPreview()
  $("msg-input").focus()
}

// ─── Media helpers ────────────────────────────────────────────────────────────
function mediaHtml(mediaUrl, mediaType) {
  if (!mediaUrl) return ""
  if (mediaType === "image")
    return `<img class="media-img" src="${mediaUrl}" loading="lazy" onclick="window.open('${mediaUrl}','_blank')" />`
  if (mediaType === "audio")
    return `<audio class="media-audio" controls src="${mediaUrl}"></audio>`
  if (mediaType === "video")
    return `<video class="media-video" controls src="${mediaUrl}"></video>`
  return `<a class="media-link" href="${mediaUrl}" target="_blank">📎 ${mediaUrl.split("/").pop()}</a>`
}

// ─── Media upload + preview ───────────────────────────────────────────────────
window.openFilePicker = function () {
  $("file-input").click()
}


window.clearMediaPreview = function () {
  pendingMedia = null
  const bar = $("media-preview-bar")
  if (bar) { bar.innerHTML = ""; bar.style.display = "none" }
}

// ─── Message rendering ────────────────────────────────────────────────────────
function renderOutgoingMessage(to, content, clientId, messageId, tick = "pending", mediaUrl = null, mediaType = null) {
  const domId  = messageId ? `tick-${messageId}` : `tick-${clientId}`
  tickMap[clientId || messageId] = domId
  const symbol = {pending: "⏳", tick1: "✓", tick2: "✓✓", tick3: "✓✓"}[tick] || "⏳"

  const div = document.createElement("div")
  div.className = "msg-row outgoing"
  div.innerHTML = `
    <div class="bubble">
      <span class="msg-meta">→ ${escHtml(to)}</span>
      ${mediaHtml(mediaUrl, mediaType)}
      ${content ? `<span class="msg-content">${escHtml(content)}</span>` : ""}
      <span class="ticks ${tick || "pending"}" id="${domId}">${symbol}</span>
    </div>`
  $("messages").appendChild(div)
  $("messages").scrollTop = $("messages").scrollHeight
}

function renderIncomingMessage(from, content, messageId, mediaUrl = null, mediaType = null) {
  const div = document.createElement("div")
  div.className = "msg-row incoming"
  div.innerHTML = `
    <div class="bubble">
      <span class="msg-meta">${escHtml(from)}</span>
      ${mediaHtml(mediaUrl, mediaType)}
      ${content ? `<span class="msg-content">${escHtml(content)}</span>` : ""}
    </div>`
  $("messages").appendChild(div)
  $("messages").scrollTop = $("messages").scrollHeight
}

function setTick(key, symbol, cssClass) {
  const el = document.getElementById(tickMap[key])
  if (!el) return
  el.textContent = symbol
  el.className   = `ticks ${cssClass}`
}

// ─── Read receipts ────────────────────────────────────────────────────────────
function sendRead(message_id, from) {
  if (!channel) return
  log(`→ read  id=${message_id}`, "send")
  channel.push("read", {message_id, from})
}

window.addEventListener("focus", () => {
  if (!activeConv) return
  const flush = pendingRead.filter(r => r.from === activeConv || r.convId === activeConv)
  flush.forEach(r => {
    pendingRead.splice(pendingRead.indexOf(r), 1)
    sendRead(r.message_id, r.from)
  })
})

// ─── Create Group Modal ───────────────────────────────────────────────────────
window.openCreateGroupModal = function () {
  // Populate member checkboxes
  const box = $("group-member-checkboxes")
  box.innerHTML = ""
  allUsers.forEach((_v, uid) => {
    const label = document.createElement("label")
    label.className = "checkbox-option"
    label.innerHTML = `<input type="checkbox" value="${escHtml(uid)}" /> ${escHtml(uid)}`
    box.appendChild(label)
  })
  $("group-name-input").value = ""
  $("create-group-modal").style.display = "flex"
  $("group-name-input").focus()
}

window.closeCreateGroupModal = function (e) {
  if (!e || e.target === $("create-group-modal"))
    $("create-group-modal").style.display = "none"
}

window.submitCreateGroup = function () {
  const name = $("group-name-input").value.trim()
  if (!name) { alert("Enter a group name"); return }

  const members = [...$("group-member-checkboxes").querySelectorAll("input:checked")]
    .map(el => el.value)

  channel.push("create_group", {name, members})
    .receive("ok", ({group_id, name: gname}) => {
      myGroups.set(group_id, {name: gname})
      log(`◆ Group "${gname}" created | id=${group_id}`, "system")
      renderSidebar()
      $("create-group-modal").style.display = "none"
      openConversation(group_id, "group")
    })
    .receive("error", r => log(`Create group failed: ${JSON.stringify(r)}`, "error"))
}

// ─── Manage Members Modal ─────────────────────────────────────────────────────
window.openManageMembersModal = function () {
  if (activeConvType !== "group") return

  // Reset
  $("current-members-list").innerHTML = "<em style='color:#6c7086;font-size:13px'>Loading…</em>"
  $("add-member-list").innerHTML = ""
  $("manage-members-modal").style.display = "flex"

  // Fetch current members from server
  channel.push("get_group_members", {group_id: activeConv})
    .receive("ok", ({members}) => {
      const currentIds = new Set(members.map(m => m.user_id))

      // Render current members with remove button
      const currentList = $("current-members-list")
      currentList.innerHTML = ""
      if (members.length === 0) {
        currentList.innerHTML = "<em style='color:#6c7086;font-size:13px'>No members yet</em>"
      } else {
        members.forEach(({user_id}) => {
          const row = document.createElement("div")
          row.className = "member-row"
          row.id = `member-row-${user_id}`
          row.innerHTML = `
            <span class="member-name">${escHtml(user_id)}</span>
            <button class="btn-remove" onclick="window.removeMember('${escHtml(user_id)}')">Remove</button>
          `
          currentList.appendChild(row)
        })
      }

      // Render non-members in the add section
      const addList = $("add-member-list")
      addList.innerHTML = ""
      allUsers.forEach((_v, uid) => {
        if (currentIds.has(uid)) return
        const label = document.createElement("label")
        label.className = "checkbox-option"
        label.innerHTML = `<input type="radio" name="add-member-user" value="${escHtml(uid)}" /> ${escHtml(uid)}`
        addList.appendChild(label)
      })
      if (addList.children.length === 0) {
        addList.innerHTML = "<em style='color:#6c7086;font-size:13px'>All users are already members</em>"
      }
    })
    .receive("error", r => log(`Failed to load members: ${JSON.stringify(r)}`, "error"))

  // Wire custom date picker
  document.querySelectorAll("input[name='share-history']").forEach(r => {
    r.addEventListener("change", () => {
      $("share-from-date").style.display = r.value === "custom" ? "block" : "none"
    })
  })
}

window.closeManageMembersModal = function (e) {
  if (!e || e.target === $("manage-members-modal"))
    $("manage-members-modal").style.display = "none"
}

window.removeMember = function (uid) {
  if (!confirm(`Remove ${uid} from the group?`)) return
  channel.push("remove_from_group", {group_id: activeConv, user_id: uid})
    .receive("ok", () => {
      log(`◆ ${uid} removed from group`, "system")
      const row = document.getElementById(`member-row-${uid}`)
      if (row) row.remove()
      // Move them to the add list
      const addList = $("add-member-list")
      const em = addList.querySelector("em")
      if (em) em.remove()
      const label = document.createElement("label")
      label.className = "checkbox-option"
      label.innerHTML = `<input type="radio" name="add-member-user" value="${escHtml(uid)}" /> ${escHtml(uid)}`
      addList.appendChild(label)
    })
    .receive("error", r => log(`Remove failed: ${JSON.stringify(r)}`, "error"))
}

window.submitAddMember = function () {
  const selected = document.querySelector("input[name='add-member-user']:checked")
  if (!selected) { alert("Select a user to add"); return }

  const uid      = selected.value
  const strategy = document.querySelector("input[name='share-history']:checked")?.value || "all"

  let share_from = 0
  if (strategy === "now") {
    share_from = Date.now()
  } else if (strategy === "custom") {
    const val = $("share-from-date").value
    if (!val) { alert("Pick a date"); return }
    share_from = new Date(val).getTime()
  }
  // strategy === "all" → share_from stays 0

  channel.push("add_to_group", {group_id: activeConv, user_id: uid, share_from})
    .receive("ok", () => {
      log(`◆ ${uid} added | share_from=${share_from === 0 ? "beginning" : new Date(share_from).toLocaleString()}`, "system")
      $("manage-members-modal").style.display = "none"
    })
    .receive("error", r => log(`Add member failed: ${JSON.stringify(r)}`, "error"))
}

// ─── Typing indicator ─────────────────────────────────────────────────────────
const typingClearTimers = new Map()   // conv_id → setTimeout handle
let lastTypingSent      = 0           // throttle: last time we sent a typing event

function showTyping(from, convId) {
  if (activeConv !== convId) return
  const indicator = $("typing-indicator")
  $("typing-text").textContent = `${from} is typing`
  indicator.style.display = "flex"

  // Auto-clear after 3s — reset if a new event arrives before then
  if (typingClearTimers.has(convId)) clearTimeout(typingClearTimers.get(convId))
  typingClearTimers.set(convId, setTimeout(() => {
    if (activeConv === convId) indicator.style.display = "none"
    typingClearTimers.delete(convId)
  }, 3000))
}

function clearTyping() {
  $("typing-indicator").style.display = "none"
  typingClearTimers.forEach(t => clearTimeout(t))
  typingClearTimers.clear()
}

// ─── Keyboard + file input ────────────────────────────────────────────────────
document.addEventListener("DOMContentLoaded", () => {
  $("msg-input")?.addEventListener("keydown", e => {
    if (e.key === "Enter" && !e.shiftKey) { e.preventDefault(); window.sendMessage(); return }

    // Throttle: send at most 1 typing event every 2s
    if (!channel || !activeConv) return
    const now = Date.now()
    if (now - lastTypingSent < 2000) return
    lastTypingSent = now

    if (activeConvType === "group") {
      channel.push("typing", {group_id: activeConv})
    } else {
      channel.push("typing", {to: activeConv})
    }
  })
  $("username-input")?.addEventListener("keydown", e => {
    if (e.key === "Enter") window.connect()
  })

  $("file-input")?.addEventListener("change", e => {
    const file = e.target.files[0]
    if (!file) return
    e.target.value = ""

    const MAX_BYTES = 200 * 1024 * 1024
    if (file.size > MAX_BYTES) {
      log(`✗ file too large (${(file.size/1024/1024).toFixed(1)} MB, max 200 MB)`, "error")
      return
    }

    const bar = $("media-preview-bar")
    bar.style.display = "flex"

    // Use XMLHttpRequest so we can show real upload progress
    const xhr = new XMLHttpRequest()
    const form = new FormData()
    form.append("file", file)

    xhr.upload.addEventListener("progress", ev => {
      if (!ev.lengthComputable) return
      const pct = Math.round(ev.loaded / ev.total * 100)
      bar.innerHTML = `<span class="upload-status">Uploading ${escHtml(file.name)}… ${pct}%
        <span class="upload-progress-track"><span class="upload-progress-bar" style="width:${pct}%"></span></span>
      </span>`
    })

    xhr.addEventListener("load", () => {
      try {
        const {url, media_type, error} = JSON.parse(xhr.responseText)
        if (error || xhr.status >= 400) {
          bar.innerHTML = `<span class="upload-error">✗ ${escHtml(error || "Upload failed")}</span>`
          return
        }
        pendingMedia = {url, mediaType: media_type, name: file.name}
        bar.innerHTML = mediaHtml(url, media_type) +
          `<button class="media-cancel-btn" onclick="window.clearMediaPreview()">✕</button>`
        $("msg-input").focus()
        log(`↑ uploaded ${media_type} (${(file.size/1024/1024).toFixed(1)} MB)`, "system")
      } catch (_) {
        bar.innerHTML = `<span class="upload-error">✗ Upload failed</span>`
      }
    })

    xhr.addEventListener("error", () => {
      bar.innerHTML = `<span class="upload-error">✗ Upload failed — check connection</span>`
      log("✗ upload failed", "error")
    })

    xhr.open("POST", "/api/upload")
    xhr.send(form)
    bar.innerHTML = `<span class="upload-status">Uploading ${escHtml(file.name)}… 0%</span>`
  })
})
