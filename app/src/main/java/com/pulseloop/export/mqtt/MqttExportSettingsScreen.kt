package com.pulseloop.export.mqtt

import androidx.compose.foundation.layout.*
import androidx.compose.foundation.rememberScrollState
import androidx.compose.foundation.text.KeyboardOptions
import androidx.compose.foundation.verticalScroll
import androidx.compose.material.icons.Icons
import androidx.compose.material.icons.automirrored.filled.ArrowBack
import androidx.compose.material3.*
import androidx.compose.runtime.*
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.platform.LocalContext
import androidx.compose.ui.text.font.FontWeight
import androidx.compose.ui.text.input.KeyboardType
import androidx.compose.ui.text.input.PasswordVisualTransformation
import androidx.compose.ui.unit.dp
import com.pulseloop.ui.theme.PulseColors
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import java.text.DateFormat
import java.util.Date

/**
 * Settings → MQTT export. Fields save as they're edited; the password field starts empty and only
 * writes when typed into, so the stored password is never read back onto the screen.
 */
@OptIn(ExperimentalMaterial3Api::class)
@Composable
fun MqttExportSettingsScreen(onBack: () -> Unit) {
    val context = LocalContext.current
    val scope = rememberCoroutineScope()
    val settings = remember { MqttExportSettings(context) }
    val stateStore = remember { RoomExportStateStore(MqttExportDatabase.get(context)) }
    val status by stateStore.statusFlow().collectAsState(initial = ExportStatus())

    var enabled by remember { mutableStateOf(settings.enabled) }
    var host by remember { mutableStateOf(settings.host) }
    var port by remember { mutableStateOf(settings.port.toString()) }
    var username by remember { mutableStateOf(settings.username) }
    var password by remember { mutableStateOf("") }
    val passwordSet = remember(password) { settings.password.isNotEmpty() }
    var clientId by remember { mutableStateOf(settings.clientId) }
    var baseTopic by remember { mutableStateOf(settings.baseTopic) }
    var tls by remember { mutableStateOf(settings.tls) }

    var testing by remember { mutableStateOf(false) }
    var testResult by remember { mutableStateOf<MqttExporter.Companion.TestResult?>(null) }
    var confirmReset by remember { mutableStateOf(false) }

    // The queue count is computed from Room, so refresh it whenever the screen opens.
    LaunchedEffect(Unit) {
        withContext(Dispatchers.IO) { runCatching { MqttExportWorker.exporter(context).refreshPending() } }
    }

    Scaffold(
        containerColor = PulseColors.background,
        contentWindowInsets = WindowInsets(0, 0, 0, 0),
        topBar = {
            TopAppBar(
                title = { Text("MQTT Export") },
                navigationIcon = { IconButton(onClick = onBack) { Icon(Icons.AutoMirrored.Filled.ArrowBack, "Back") } },
                windowInsets = WindowInsets(0, 0, 0, 0),
                colors = TopAppBarDefaults.topAppBarColors(containerColor = PulseColors.background),
            )
        },
    ) { padding ->
        Column(
            Modifier.fillMaxSize().padding(padding).verticalScroll(rememberScrollState()).padding(16.dp),
            verticalArrangement = Arrangement.spacedBy(16.dp),
        ) {
            Card(Modifier.fillMaxWidth()) {
                Column(Modifier.padding(16.dp), verticalArrangement = Arrangement.spacedBy(8.dp)) {
                    Row(verticalAlignment = Alignment.CenterVertically) {
                        Column(Modifier.weight(1f)) {
                            Text("Publish to MQTT", style = MaterialTheme.typography.titleMedium, fontWeight = FontWeight.SemiBold)
                            Text(
                                "Sends every ring reading, with its original time, to your broker after each sync. " +
                                    "Home Assistant discovers the sensors automatically. Nothing leaves your network.",
                                style = MaterialTheme.typography.bodySmall,
                                color = MaterialTheme.colorScheme.onSurfaceVariant,
                            )
                        }
                        Switch(checked = enabled, onCheckedChange = {
                            enabled = it
                            settings.enabled = it
                            if (it) MqttExportWorker.enqueue(context, delaySeconds = 0) else MqttExportWorker.cancel(context)
                        })
                    }
                }
            }

            Card(Modifier.fillMaxWidth()) {
                Column(Modifier.padding(16.dp), verticalArrangement = Arrangement.spacedBy(8.dp)) {
                    Text("Broker", style = MaterialTheme.typography.titleMedium, fontWeight = FontWeight.SemiBold)
                    OutlinedTextField(
                        value = host, onValueChange = { host = it; settings.host = it },
                        label = { Text("Host") }, placeholder = { Text("10.28.107.15") },
                        singleLine = true, modifier = Modifier.fillMaxWidth(),
                        keyboardOptions = KeyboardOptions(keyboardType = KeyboardType.Uri),
                    )
                    OutlinedTextField(
                        value = port,
                        onValueChange = { v ->
                            port = v.filter { it.isDigit() }.take(5)
                            port.toIntOrNull()?.let { settings.port = it }
                        },
                        label = { Text("Port") }, placeholder = { Text("${MqttExportSettings.DEFAULT_PORT}") },
                        singleLine = true, modifier = Modifier.fillMaxWidth(),
                        keyboardOptions = KeyboardOptions(keyboardType = KeyboardType.Number),
                        isError = port.toIntOrNull()?.let { it !in 1..65535 } ?: true,
                    )
                    OutlinedTextField(
                        value = username, onValueChange = { username = it; settings.username = it },
                        label = { Text("Username") }, singleLine = true, modifier = Modifier.fillMaxWidth(),
                    )
                    OutlinedTextField(
                        value = password, onValueChange = { password = it; settings.password = it },
                        label = { Text("Password") },
                        placeholder = { Text(if (passwordSet) "Saved — type to replace" else "") },
                        singleLine = true, modifier = Modifier.fillMaxWidth(),
                        visualTransformation = PasswordVisualTransformation(),
                        keyboardOptions = KeyboardOptions(keyboardType = KeyboardType.Password),
                    )
                    OutlinedTextField(
                        value = clientId, onValueChange = { clientId = it; settings.clientId = it },
                        label = { Text("Client ID") }, singleLine = true, modifier = Modifier.fillMaxWidth(),
                    )
                    OutlinedTextField(
                        value = baseTopic, onValueChange = { baseTopic = it; settings.baseTopic = it },
                        label = { Text("Base topic") }, placeholder = { Text(MqttExportSettings.DEFAULT_BASE_TOPIC) },
                        singleLine = true, modifier = Modifier.fillMaxWidth(),
                    )
                    Row(verticalAlignment = Alignment.CenterVertically) {
                        Text("Use TLS", Modifier.weight(1f))
                        Switch(checked = tls, onCheckedChange = { tls = it; settings.tls = it })
                    }

                    Button(
                        enabled = !testing && settings.isConfigured,
                        onClick = {
                            testing = true
                            testResult = null
                            scope.launch {
                                testResult = withContext(Dispatchers.IO) { MqttExporter.testConnection(settings.config()) }
                                testing = false
                            }
                        },
                    ) {
                        if (testing) CircularProgressIndicator(Modifier.size(16.dp), strokeWidth = 2.dp)
                        else Text("Test connection")
                    }
                    when (val r = testResult) {
                        MqttExporter.Companion.TestResult.Success ->
                            Text("Connected and authenticated.", color = PulseColors.success, fontWeight = FontWeight.SemiBold)
                        is MqttExporter.Companion.TestResult.Failure ->
                            Text(r.error.message ?: r.error.kind.label, color = PulseColors.danger, fontWeight = FontWeight.SemiBold)
                        null -> {}
                    }
                }
            }

            Card(Modifier.fillMaxWidth()) {
                Column(Modifier.padding(16.dp), verticalArrangement = Arrangement.spacedBy(6.dp)) {
                    Text("Status", style = MaterialTheme.typography.titleMedium, fontWeight = FontWeight.SemiBold)
                    StatusLine("Last publish", status.lastPublishAt?.let(::formatTime) ?: "Never")
                    StatusLine("Last attempt", status.lastAttemptAt?.let(::formatTime) ?: "Never")
                    StatusLine("Queued", "${status.pending} reading${if (status.pending == 1) "" else "s"}")
                    StatusLine("History backfill", if (status.backfillCompletedAt != null) "Complete" else "Not yet")
                    status.lastError?.let {
                        Text("Last error: $it", color = PulseColors.danger, style = MaterialTheme.typography.bodySmall)
                    }
                    Row(horizontalArrangement = Arrangement.spacedBy(8.dp)) {
                        OutlinedButton(enabled = enabled, onClick = { MqttExportWorker.enqueue(context, delaySeconds = 0) }) {
                            Text("Export now")
                        }
                        OutlinedButton(enabled = enabled, onClick = { confirmReset = true }) { Text("Re-export all") }
                    }
                    Text(
                        "Retries automatically on the next sync, when the app opens, or when the network returns.",
                        style = MaterialTheme.typography.bodySmall,
                        color = MaterialTheme.colorScheme.onSurfaceVariant,
                    )
                }
            }
        }
    }

    if (confirmReset) {
        AlertDialog(
            onDismissRequest = { confirmReset = false },
            title = { Text("Re-export all history?") },
            text = { Text("Forgets what has been sent and publishes every stored reading again. Consumers of the history topics will see duplicates.") },
            confirmButton = {
                TextButton(onClick = {
                    confirmReset = false
                    scope.launch {
                        withContext(Dispatchers.IO) { stateStore.reset() }
                        MqttExportWorker.enqueue(context, delaySeconds = 0)
                    }
                }) { Text("Re-export") }
            },
            dismissButton = { TextButton(onClick = { confirmReset = false }) { Text("Cancel") } },
        )
    }
}

@Composable
private fun StatusLine(label: String, value: String) {
    Row(Modifier.fillMaxWidth()) {
        Text(label, Modifier.weight(1f), color = MaterialTheme.colorScheme.onSurfaceVariant)
        Text(value)
    }
}

private fun formatTime(ms: Long): String =
    DateFormat.getDateTimeInstance(DateFormat.MEDIUM, DateFormat.SHORT).format(Date(ms))
