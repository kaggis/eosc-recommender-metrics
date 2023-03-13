import './App.css';
import logo from './grnet.png';
import * as React from 'react';
import { Routes, Route, Outlet, NavLink } from 'react-router-dom';


const App = () => {
  return (
    <Routes>
      <Route element={<Layout />}>
        <Route index element={<Metrics />} />
        <Route path="kpis" element={<KPIS />} />
        <Route path="graphs" element={<Graphs />} />
        {/* TODO: Add dynamic routes for documentation views */}
      </Route>
    </Routes>
  );
};

const Layout = () => {
  

  return (
    <>
      <section className="wrapper">
      <aside>
        <nav>
          <ul>
            <li><img src={logo} alt="logo" className="logo"/></li>
            <li><span className="nav-category">METRICS DASHBOARD</span></li>
            <li><NavLink to="/">Metrics</NavLink></li>
            <li><NavLink to="/kpis">KPIs</NavLink></li>
            <li><NavLink to="/graphs">Graphs</NavLink></li>
            <li><span className="nav-category">METRICS DOCUMENTATION</span></li>
          </ul>
        </nav>
      </aside>
      <main>
        <Outlet />
      </main>
      </section>
    </>
  );
};

// This is the main metrics view component - can be extended to its own file
function Metrics() {
  return (
    <>
      <h2>Metrics</h2>
    </>
  );
};

// This is the KPIS view component - can be extended to its own file
function KPIS() {
  return (
    <>
      <h2>KPIs</h2>
    </>
  );
};

// This is the Graphs view component - can be extended to its own file
function Graphs() {
  return (
    <>
      <h2>Graphs</h2>
    </>
  );
};

// TODO: Add component to generate dynamic views for metrics documentation
export default App;