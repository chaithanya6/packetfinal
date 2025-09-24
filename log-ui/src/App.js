import React, { useEffect, useState } from "react";
import {
  ResponsiveContainer,
  BarChart,
  Bar,
  XAxis,
  YAxis,
  Tooltip,
  Legend,
} from "recharts";
import "./index.css";

const API_BASE = "/api"; // ✅ Always go through nginx proxy

function App() {
  const [logs, setLogs] = useState([]);
  const [view, setView] = useState("table");
  const [clients, setClients] = useState([]);
  const [selectedClient, setSelectedClient] = useState("All");
  const [timeRange, setTimeRange] = useState("All");

  const fetchData = async () => {
    try {
      const logsRes = await fetch(`${API_BASE}/logs?limit=1000`);
      const logsData = await logsRes.json();
      const allLogs = logsData.logs || [];
      setLogs(allLogs);

      // Collect unique client names
      const uniqueClients = Array.from(
        new Set(allLogs.map((log) => log.client_name || "unknown"))
      );
      setClients(uniqueClients);
    } catch (err) {
      console.error("Error fetching logs:", err);
    }
  };

  useEffect(() => {
    fetchData();
    const interval = setInterval(fetchData, 10000); // auto-refresh every 10s
    return () => clearInterval(interval);
  }, []);

  const colorForLevel = (level) => {
    switch (level) {
      case "ERROR":
        return "red";
      case "WARNING":
        return "orange";
      case "INFO":
        return "green";
      case "DEBUG":
        return "deepskyblue";
      default:
        return "white";
    }
  };

  // Filter logs by client
  let filteredLogs =
    selectedClient === "All"
      ? logs
      : logs.filter((log) => log.client_name === selectedClient);

  // Apply time filter
  if (timeRange !== "All") {
    const now = new Date();
    let cutoff = new Date();
    if (timeRange === "5m") cutoff.setMinutes(now.getMinutes() - 5);
    if (timeRange === "30m") cutoff.setMinutes(now.getMinutes() - 30);
    if (timeRange === "1h") cutoff.setHours(now.getHours() - 1);

    filteredLogs = filteredLogs.filter(
      (log) => new Date(log.timestamp) >= cutoff
    );
  }

  // Build counts dynamically based on filtered logs
  const counts = filteredLogs.reduce((acc, log) => {
    acc[log.level] = (acc[log.level] || 0) + 1;
    return acc;
  }, {});

  const chartData = Object.entries(counts).map(([level, count]) => ({
    level,
    count,
  }));

  return (
    <div className="App">
      <h1>📊 Log Dashboard</h1>
      <div className="controls">
        <button
          className={view === "table" ? "active" : ""}
          onClick={() => setView("table")}
        >
          Table View
        </button>
        <button
          className={view === "graph" ? "active" : ""}
          onClick={() => setView("graph")}
        >
          Graph View
        </button>
        <button onClick={fetchData}>🔄 Refresh</button>

        {/* Client Filter */}
        <select
          value={selectedClient}
          onChange={(e) => setSelectedClient(e.target.value)}
        >
          <option value="All">All Clients</option>
          {clients.map((client) => (
            <option key={client} value={client}>
              {client}
            </option>
          ))}
        </select>

        {/* Time Range Filter */}
        <select
          value={timeRange}
          onChange={(e) => setTimeRange(e.target.value)}
        >
          <option value="All">All Time</option>
          <option value="5m">Last 5 min</option>
          <option value="30m">Last 30 min</option>
          <option value="1h">Last 1 hour</option>
        </select>
      </div>

      {view === "table" && (
        <table>
          <thead>
            <tr>
              <th>Timestamp</th>
              <th>Level</th>
              <th>Message</th>
              <th>Client</th>
            </tr>
          </thead>
          <tbody>
            {filteredLogs.map((log) => (
              <tr key={log.event_id}>
                <td>{log.timestamp}</td>
                <td
                  style={{
                    color: colorForLevel(log.level),
                    fontWeight: "bold",
                  }}
                >
                  {log.level}
                </td>
                <td>{log.message}</td>
                <td>{log.client_name}</td>
              </tr>
            ))}
          </tbody>
        </table>
      )}

      {view === "graph" && (
        <ResponsiveContainer width="100%" height={400}>
          <BarChart data={chartData}>
            <XAxis dataKey="level" />
            <YAxis />
            <Tooltip />
            <Legend />
            <Bar dataKey="count" fill="#8884d8" />
          </BarChart>
        </ResponsiveContainer>
      )}
    </div>
  );
}

export default App;

