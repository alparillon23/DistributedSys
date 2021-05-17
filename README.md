# Distributed Systems - Key Value Store

Distributed Key Value Store.<br>Supports multiple clients in parallel and multiple storage support with RAID1 topology<br>
Uses Go RPC remote calls<br>

<h4>Deployment Description - Credit to Ivan Beschastnikh (Instructor)</h4>
<a href="https://www.cs.ubc.ca/~bestchai/teaching/cs416_2020w2/assign5/index.html">Part 1</a><br>
<a href="https://www.cs.ubc.ca/~bestchai/teaching/cs416_2020w2/assign6/index.html">Part 2</a><br>

<h4>How to run</h4>

<ul>
  <li>Start the tracing server with <i>go run ./cmd/tracing-server</i></li>
  <li>Start the frontend with <i>go run ./cmd/frontend/</i></li>
  <li>For each storage:
    <ul>
      <li>Amend storage_config.json in config folder with desired working address</li>
      <li>Run the storage go file with <i>go run ./cmd/storage</i></li>
    </ul>
  </li>
  <li>For each client:
    <ul>
      <li>Amend client_config.json in config folder with desired working address</li>
      <li>Add desired tasks in client/main.go - examples are provided</li>
      <li>Run the client go file with <i>go run ./cmd/client</i></li>
    </ul>
  </li>
</ul>
