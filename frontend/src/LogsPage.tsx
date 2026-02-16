import React, { useEffect, useState } from "react";
import {API_URL} from "./globals.ts";
import { List } from "immutable";
import {useNavigate} from "react-router-dom";
import ReactJsonView from '@microlink/react-json-view'

type Log = {
    id?: number;
    timestamp: string;
    originTopic: string;
    data: any;
};

type LogsPageProps = {
    profileIDs: string;
    topics: string;
};

type LogMsgProps = {
    log: Log;
    isSelected: boolean;
    setSelectedLog: (log: Log | null) => void;
}

const LogMsg: React.FC<LogMsgProps> = ({ log, isSelected, setSelectedLog }) => {
    const onClick = () => setSelectedLog(log);

    const [show, setShow] = useState(false);

    useEffect(() => {
        setShow(true);
    }, []);

    return (
        <div className={`log-container box ${show ? "enter" : ""} ${isSelected ? "log-container-selected" : ""}`} onClick={onClick}>
            <div className="log-header-wrapper">
                <div className="log-header log-timestamp">
                    {log.timestamp}
                </div>
                <div className="log-header log-topic">
                    {log.originTopic}
                </div>
                <div className="log-header log-text">
                    {JSON.stringify(log.data)}
                </div>
            </div>
        </div>
    );
};

let LOG_KEY = 0;

export const LogsPage: React.FC<LogsPageProps> = ({ profileIDs, topics }) => {
    const navigate = useNavigate();

    const [logs, setLogs] = useState<List<Log>>(List());
    const [selectedLog, setSelectedLog] = useState<Log | null>(null);

    useEffect(() => {
        setLogs(List());
        setSelectedLog(null);

        const tailLogQuery = `?profileIDs=${profileIDs}&topics=${topics}`;
        // @ts-ignore
        const evtSource = new EventSource(`${API_URL}/taillog${tailLogQuery}`, { cors: "true" });

        const handleLog = (event: MessageEvent) => {
            try {
                const wireLog: Log = JSON.parse(event.data);
                wireLog.id = LOG_KEY++;
                setLogs((prev) => prev.unshift(wireLog));
            } catch (err) {
                console.error("Failed to parse log event:", err);
            }
        };
        evtSource.addEventListener("log", handleLog);

        return () => {
            evtSource.removeEventListener("log", handleLog);
            evtSource.close();
        };
    }, [profileIDs, topics]);

    return (
        <React.Fragment>
            <div className="banner"></div>

            <div className="container">
                <div className="form-container">
                    <form
                        method="GET"
                        action=""
                        onSubmit={(e) => {
                            e.preventDefault();
                            const formData = new FormData(e.currentTarget);
                            const newProfileIDs = formData.get("profileIDs")?.toString();
                            const newTopics = formData.get("topics")?.toString();
                            navigate("/logs?profileIDs=" + newProfileIDs + "&topics=" + newTopics, { replace: true });
                        }}
                    >
                        <div className="form-element">
                            <label>
                                <span className="label-text">Profiles</span>
                                <input
                                    className="text-input"
                                    type="text"
                                    name="profileIDs"
                                    defaultValue={profileIDs}
                                />
                            </label>
                            <div className="label-tooltip">
                                *Enter PIDs to subscribe to, separated by commas
                            </div>
                        </div>

                        <div className="form-element">
                            <label>
                                <span className="label-text">Topics</span>
                            </label>
                            <div className="input-anchor">
                                <input
                                    className="text-input"
                                    type="text"
                                    name="topics"
                                    defaultValue={topics}
                                />
                            </div>
                            <div className="label-tooltip">
                                *Enter topics (connections, accounts, transactions, holdings) to
                                subscribe to, separated by commas
                            </div>
                        </div>

                        <button type="submit">Subscribe</button>
                    </form>

                    {selectedLog !== null && (
                        <div className="json-viewer">
                            <ReactJsonView
                                src={selectedLog.data}
                                theme={{
                                    // Background layers (top)
                                    base00: "#0b0f1a",   // deepest background (midnight)
                                    base01: "#141a2a",   // slightly lighter panel bg
                                    base02: "#1c2336",   // borders / subtle contrast
                                    base03: "#2a3350",   // comments / muted text

                                    // Foreground text
                                    base04: "#b6c3ff",   // soft periwinkle
                                    base05: "#d6dcff",   // main text (light lavender)
                                    base06: "#f2f4ff",   // bright text highlights
                                    base07: "#ffffff",   // pure white accents

                                    // Accent colors (bottom = components / syntax)
                                    base08: "#ff6b9d",   // pink-red (errors / strong highlight)
                                    base09: "#ff9f43",   // orange (numbers / constants)
                                    base0A: "#ffd86b",   // warm gold (warnings / key accents)
                                    base0B: "#4cd4ff",   // neon sky blue (strings / success)
                                    base0C: "#7a5cff",   // electric purple (special / regex)
                                    base0D: "#5aa7ff",   // bright blue (functions / links)
                                    base0E: "#c792ff",   // lavender purple (keywords)
                                    base0F: "#ff884d"    // deep orange (secondary accent)
                                }}
                            />
                        </div>
                    )}
                </div>

                <div className="logs-container">
                    {logs.map(log => (
                        <LogMsg
                            key={log.id}
                            log={log}
                            isSelected={selectedLog !== null && selectedLog.id === log.id}
                            setSelectedLog={setSelectedLog}
                        />
                    ))}
                </div>
            </div>
        </React.Fragment>
    );
};