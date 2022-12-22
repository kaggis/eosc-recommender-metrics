import {Container, Row, Col, Nav } from "react-bootstrap";

function App() {
  return (
    <>
     <Container fluid >
        <Row>
            <Col xs={2}>      
              <Nav className="col-md-12 d-none d-md-block bg-light sidebar">
                <h4 className="pt-4">RS Eval</h4>
                <hr/>
                <span>Metrics Views</span>
                <Nav.Item>
                    <Nav.Link>RS Metrics</Nav.Link>
                </Nav.Item>
                <Nav.Item>
                    <Nav.Link>KPIs</Nav.Link>
                </Nav.Item>
                <span>Metrics Documentation</span>
                <Nav.Item>
                    <Nav.Link>Accuracy</Nav.Link>
                </Nav.Item>
                <Nav.Item>
                    <Nav.Link>Catalog Coverage</Nav.Link>
                </Nav.Item>
                <Nav.Item>
                    <Nav.Link>Catalog Coverage</Nav.Link>
                </Nav.Item>
                <Nav.Item>
                    <Nav.Link>Click-Through Rate</Nav.Link>
                </Nav.Item>
                <Nav.Item>
                    <Nav.Link>Diversity Shannon Entropy</Nav.Link>
                </Nav.Item>
                <Nav.Item>
                    <Nav.Link>Diversity Gini Index</Nav.Link>
                </Nav.Item>
                <Nav.Item>
                    <Nav.Link>Hit Rate</Nav.Link>
                </Nav.Item>
                <Nav.Item>
                    <Nav.Link>Novelty</Nav.Link>
                </Nav.Item>
                <Nav.Item>
                    <Nav.Link>User Coverage</Nav.Link>
                </Nav.Item>
              </Nav>
            </Col>
            <Col  xs={10}>
                <h3 className="pt-4">Metrics</h3>
            </Col> 
        </Row>
      </Container>
    </>
  );
}

export default App;
