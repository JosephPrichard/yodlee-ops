import React from "react";
import {BrowserRouter as Router, Route, Routes, useNavigate, useSearchParams} from "react-router-dom";
import {LogsPage} from "./LogsPage";
import {ProfileTree} from "./ProfileTree.tsx";
import {worker} from "./mocks/browser";
import "react-data-grid/lib/styles.css";

const LogsPageWrapper: React.FC = () => {
    const [searchParams] = useSearchParams();

    const profileIDs = searchParams.get("profileIDs")?.toString() ?? "";
    let subjects = searchParams.get("subjects")?.toString() ?? "";

    if (subjects === "") {
        subjects = "connections,accounts,transactions,holdings";
    }

    return <LogsPage profileIDs={profileIDs} subjects={subjects}/>;
};

const ProfileTreeWrapper: React.FC = () => {
    const [searchParams] = useSearchParams();

    const profileIDs = searchParams.get("profileIDs")?.toString() ?? "";

    return <ProfileTree profileIDs={profileIDs}/>;
};

const HomePage: React.FC = () => {
    const navigate = useNavigate();

    const goToProfileTree = () => navigate("/profiletree");
    const goToLogs = () => navigate("/logs");

    return (
        <div style={{display: "flex", justifyContent: "center", alignItems: "center", height: "100vh", flexDirection: "column"}}>
            <h1>Yodlee OPs Admin Dashboard</h1>
            <div className="button-container">
                <button onClick={goToProfileTree} style={{padding: "10px 20px", fontSize: "16px", cursor: "pointer"}}>
                    Profile Tree
                </button>
                <button onClick={goToLogs} style={{padding: "10px 20px", fontSize: "16px", cursor: "pointer"}}>
                    Log Stream
                </button>
            </div>
        </div>
    );
};

export const App: React.FC = () => {
    worker.start().then(() => console.log("Mock server started!"));

    return (
        <Router>
            <Routes>
                <Route path="/" element={<HomePage/>}/>
                <Route path="/logs" element={<LogsPageWrapper/>}/>
                <Route path="/profiletree" element={<ProfileTreeWrapper/>}/>
            </Routes>
        </Router>
    );
};