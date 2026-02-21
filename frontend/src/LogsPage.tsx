import React, { useEffect, useState } from "react";
import {API_URL, JSON_VIEW_THEME} from "./globals.ts";
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
    subjects: string;
};

type LogMsgProps = {
    log: Log;
    isSelected: boolean;
    setSelectedLog: (log: Log | null) => void;
};

function toPascalCase(str: string): string {
    return str
        .split('-')
        .map(word => word.charAt(0).toUpperCase() + word.slice(1))
        .join('');
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

export const LogsPage: React.FC<LogsPageProps> = ({ profileIDs, subjects }) => {
    const navigate = useNavigate();

    const [logs, setLogs] = useState<List<Log>>(List());
    const [selectedLog, setSelectedLog] = useState<Log | null>(null);

    useEffect(() => {
        setLogs(List());
        setSelectedLog(null);

        const tailLogQuery = `?profileIDs=${profileIDs}&subjects=${subjects}`;
        const evtSource = new EventSource(`${API_URL}/events/taillog${tailLogQuery}`);

        const handleLog = (event: MessageEvent) => {
            try {
                const wireLog: Log = JSON.parse(event.data);
                wireLog.id = LOG_KEY++;
                wireLog.originTopic = toPascalCase(wireLog.originTopic);
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
    }, [profileIDs, subjects]);

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
                            const newTopics = formData.get("subjects")?.toString();
                            navigate("/logs?profileIDs=" + newProfileIDs + "&subjects=" + newTopics, { replace: true });
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
                                <span className="label-text">Subjects</span>
                            </label>
                            <div className="input-anchor">
                                <input
                                    className="text-input"
                                    type="text"
                                    name="subjects"
                                    defaultValue={subjects}
                                />
                            </div>
                            <div className="label-tooltip">
                                *Enter subjects (connections, accounts, transactions, holdings) to
                                subscribe to, separated by commas
                            </div>
                        </div>

                        <button type="submit">Subscribe</button>
                    </form>

                    {selectedLog !== null && (
                        <div className="json-viewer">
                            <ReactJsonView
                                src={selectedLog.data}
                                theme={JSON_VIEW_THEME}
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