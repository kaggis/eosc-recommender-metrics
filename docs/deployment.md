# Licence

<! --- SPDX-License-Identifier: CC-BY-4.0  -- >

## Deployment

The service is composed of three integral components. The Preprocessor, responsible for fetching data from diverse sources in batch or real-time mode and storing it in a MongoDB database. Following that, the RSmetrics component engages in intricate metric computations. The third facet entails a RESTful API coupled with a user-friendly UI dashboard, seamlessly delivering reports as a web service and visually presenting metrics.

For deployment, ensure the following:

* Set up a virtual machine (VM) for the retrieval agent along with the MongoDB Server.
* Allocate sufficient storage for accommodating the incoming data.
* Dedicate a VM for the intricate computations performed by RSmetrics.
* Establish another VM for exposing the API and hosting the user interface (UI) dashboard.
