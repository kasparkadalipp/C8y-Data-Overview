fetch('network.json').then(response => response.json()).then(data => {
console.log(data)
const width = window.innerWidth;
const height = window.innerHeight;
const colorScale = d3.scaleOrdinal(d3.schemeCategory10);
const screenCenter = [width / 2, height / 2]
const clickedNodes = {};

// ------------------- adjacent nodes --------------------
const adjacencyList = {};
const outgoingEdges = {};
const groupMapping = {};

data.nodes.forEach(node => {
    adjacencyList[node.id] = [];
    outgoingEdges[node.id] = [];
    groupMapping[node.id] = node.group
});
  
data.links.forEach(link => {
    outgoingEdges[link.source].push(link.target);
    adjacencyList[link.source].push(link.target);
    adjacencyList[link.target].push(link.source); // Only if the graph is undirected
});
// ------------------- adjacent nodes --------------------


// --------------- measurement specific field "deviceCount" -------------
// --------------- font scaling based on "deviceCount"-------------------
const groupToValues  = d3.group(data.nodes, d => d.group) // Different scale for every group
const scales = new Map();
groupToValues.forEach((values, group) => {
    const extent = d3.extent(values, d => d.deviceCount); // Compute extent for current group
    scales.set(group, d3.scalePow().exponent(2).domain(extent).range([16, 40])); // Create a scale for the current group
});
data.nodes.forEach(node => {
    node.fontSize = scales.get(node.group)(node.deviceCount);
});


// const fontScale = d3.scaleLinear()
//     .domain([0, d3.max(data.nodes, d => d.group === 0 ? 1 : d.deviceCount)])
//     .range([14, 56])
// --------------- font scaling END---------------------


// append the svg object to the body of the page
const svg = d3.select("#container")
    .append("svg")
    .attr("width", window.innerWidth)
    .attr("height", window.innerHeight)
    // .call(d3.zoom().on("zoom", (event) => { svg.attr('transform', event.transform);})).append("g");


// handle window zoom
window.addEventListener("resize", ()  => svg
    .attr("width", window.innerWidth)
    .attr("height", window.innerHeight));

// =============================== NETWORK GRAPH START ==============================================
const networkGraph = svg.append("g");

//Zoom behaviour
svg.call(d3.zoom().on("zoom", event => networkGraph.attr('transform', event.transform)));
d3.select("svg").on("dblclick.zoom", null);


const link = networkGraph
    .selectAll("line")
    .data(data.links)
    .join("line")
    .style("stroke", "#d4d4d4")


const groupForceRadial = {
    0 : 0,
    1: 300,
    2: 700,
    3: 1400,
    4: 2200,
    5: 2800,
    6: 3800
}

const groupColors = {
    0 : '#8C564B',
    1: '#9467BD',
    2: '#D62728',
    3: '#1F77B4',
    4: '#FF7F0E',
    5: '#2CA02C',
    6: '#000000'
}


// Let's list the force we wanna apply on the network
const simulation = d3.forceSimulation(data.nodes)         // Force algorithm is applied to data.nodes
    .force("link", d3.forceLink()                         // This force provides links between nodes
        .id(d => d.id)                                    // This provide  the id of a node
        .links(data.links)                                // and this the list of links
        .strength(.1)                                
    )
    //.force("charge", d3.forceManyBody().strength(d => d.group * -400)) 
    .force("charge", d3.forceManyBody().strength(-400))         // This adds repulsion between nodes. Play with the -400 for the repulsion strength
    .force("center", d3.forceCenter(...screenCenter).strength(.1))     // This force attracts nodes to the center of the svg area
    .force("collide", d3.forceCollide(50).strength(1))
    .force("collide", rectangleCollide())
    .force("radical", d3.forceRadial(d => groupForceRadial[d.group], ...screenCenter).strength(.9))
    // .force("radical", d3.forceRadial(d => d.group ** 1.3 * 600, ...screenCenter).strength(.4))
    // .force("radical2", d3.forceRadial(d => d.group ** 1.1 * 600, ...screenCenter).strength(.4))
    .on("tick", ticked);



// Initialize the nodes
const node = networkGraph
    .selectAll("text")
    .data(data.nodes)
    .join("text")
    .text(d => d.name) // display name field
    // .style("fill", d => colorScale(d.group))
    .style("fill", d => groupColors[d.group])
    .style("font-weight", "bold")
    .style("font-size", d=>d.fontSize + "px")
    .style("cursor", "default")
    .attr("text-anchor", "middle") // Center the text on its coordinates
    .attr("dominant-baseline", "central") // Vertically center
    .call(drag(simulation))
    .each(function(d) { // text dimensions
        const bbox = this.getBBox();
        d.width = bbox.width + 40
        d.height = bbox.height + 40
    });



// ---------------- MOUSE HOVER END ---------------------------

let hoverTimeout;
let isHoverActive = false;
let isDragActive = false
const tooltip = d3.select("#tooltip");


function getTooltip(d){
    console.log("hello")
    const data = {};
    if (d.deviceCount != null) {
        data["Device count"] = d.deviceCount.toLocaleString();
    }

    if (d.frequency != null) {
        data["Measurement frequency"] = d.frequency;
    }

    if (d.dataCount != null) {
        data["Measurement count"] = d.dataCount.toLocaleString();
    }

    const uniqueFields = outgoingEdges[d.id].length
    if(d.group === 0){
        data['Unique device types'] = uniqueFields
    }
    if(d.group === 1){
        data['Unique measurement types'] = uniqueFields
    }
    if(d.group === 2){
        data['Unique fragments'] = uniqueFields
    }
    if(d.group === 3){
        data['Unique series values'] = uniqueFields
    }
    if(d.group === 4){
        data['Unique units'] = uniqueFields
    }
    return new Map(Object.entries(data)); 
}

// Update the node and link styles on mouseover
function mouseoverNode(mouseEvent, d) {
    if (isDragActive || isHoverActive) return; 
    if (hoverTimeout) clearTimeout(hoverTimeout);

    hoverTimeout = setTimeout(() => {
        isHoverActive = true;
        tooltip
            .style("display", "block")
            .html("")
            .style("white-space", "nowrap")
            .style("font-size", "20px")
            .append("table")
            ;
        
        getTooltip(d)
            .forEach((value, key) => {
                const row = tooltip.append("tr")
                row.append("td").text(key + ":")
                    .style("font-weight", "bold")
                    .style("padding-right", "10px");
                row.append("td").text(value)
        })
        updateTooltipPosition(mouseEvent);     
    }, 100);  // Delay in milliseconds
}

function mousemoveNode(event) {
    if (isHoverActive) {
        updateTooltipPosition(event);
    }
}

// Reset the node and link styles on mouseout
function mouseoutNode() {
    if (hoverTimeout) clearTimeout(hoverTimeout);
    isHoverActive = false;
    tooltip.style("display", "none");
}

node.on("mouseover", mouseoverNode)
    .on("mousemove", mousemoveNode)
    .on("mouseout", mouseoutNode)

function updateTooltipPosition(event) {
    const tooltipWidth = tooltip.node().getBoundingClientRect().width;
    const tooltipHeight = tooltip.node().getBoundingClientRect().height;

    // Get cursor position
    const x = event.pageX;
    const y = event.pageY;

    // Get the size of the window
    const windowWidth = window.innerWidth;
    const windowHeight = window.innerHeight;

    // Determine if the tooltip goes out of bounds
    const overflowX = (x + tooltipWidth > windowWidth);
    const overflowY = (y + tooltipHeight > windowHeight);

    // Adjust position to avoid overflow
    const posX = overflowX ? x - tooltipWidth : x;
    const posY = overflowY ? y - tooltipHeight : y;

    tooltip
        .style("left", `${posX}px`)  // Position tooltip to the right of the mouse cursor
        .style("top", `${posY}px`);  // Position tooltip below the mouse cursor
}

// ---------------- MOUSE HOVER END---------------------------


{ // ----------------  CLICK NODE ---------------------------
function findNodesInPathBFS(start) {
    let queue = [start];
    const visitedGroup = new Set()
    const visited = new Set();
  
    while (queue.length) {
      const currentLevel = []

      for (const vertex of queue){
        visited.add(vertex)
        visitedGroup.add(groupMapping[vertex])
        for (const adjVertex of adjacencyList[vertex]){
            if (!visited.has(adjVertex) && !visitedGroup.has(groupMapping[adjVertex]) ) {
                currentLevel.push(adjVertex)
            }
        }
      }
      queue = currentLevel
    }
    return visited;
  }



function clickNode(event, d) {
    if (d.id in clickedNodes){
        delete clickedNodes[d.id]
    }else{
        clickedNodes[d.id] = findNodesInPathBFS(d.id)  
    }

    const connectedNodes = new Set();
    for (let setValue of Object.values(clickedNodes)) {
        for (let value of setValue) {
            connectedNodes.add(value);
          }
    }
    const noneSelected = !connectedNodes.size

    node
        .style('opacity', o => noneSelected || connectedNodes.has(o.id) ? 1.0 : 0.2)
        .style('text-decoration', o => o.id in clickedNodes ? "underline" : "none");
    link
        .style('opacity', o => noneSelected || connectedNodes.has(o.source.id) && connectedNodes.has(o.target.id) ? 1.0 : 0.2)
}
node.on("click", clickNode)
} // ---------------- CLICK NODE END---------------------------

// =============================== NETWORK GRAPH END ==============================================

{ // =============================== LEGEND START ===================================================
const groupDescriptions = {
    0: "Root",
    1: "Device Type",
    2: "Measurement Type",
    3: "Fragment",
    4:"Series",
    5: "Unit",
    6: "Topic"
};

const uniqueGroups = [...new Set(data.nodes.map(node => node.group))].sort();

const legendItemHeight = 25; // Height of each legend item (color square + label)

const legend = svg.append("g")
    .attr("class", "legend")
    .attr("transform", "translate(20,20)"); // Position legend in top-left corner

// White background
legend.append("rect")
    .attr("x", -10) // padding
    .attr("y", -10) // padding
    .attr("width", 260)
    .attr("height", uniqueGroups.length * legendItemHeight + 20) 
    .attr("fill", "white")
    .attr("stroke", "black")
    .attr("stroke-width", 2);
    
// Append color square for each group
uniqueGroups.forEach((group, index) => {
    
    legend.append("rect")
        .attr("x", 0)
        .attr("y", index * legendItemHeight) // Stack items vertically
        .attr("width", 20)
        .attr("height", 20)
        .style("fill", groupColors[group]);
        // .style("fill", colorScale(group));

    // Append label for each group
    legend.append("text")
        .attr("x", 30) // Offset text to the right of the square
        .attr("y", index * legendItemHeight + 15) // Vertically align text with square
        .text(`Level ${group} - ${groupDescriptions[group]}`)
        .style("font-size", "18px")
        .style("background-color", "red")
        .attr("alignment-baseline", "middle");
});
} // =============================== LEGEND END==============================================


// This function is run at each iteration of the force algorithm, updating the nodes position.
function ticked() {
    link
        .attr("x1", d => d.source.x)
        .attr("y1", d => d.source.y)
        .attr("x2", d => d.target.x)
        .attr("y2", d => d.target.y);

    node
        .attr("cx", d => d.x + 6) // circle
        .attr("cy", d => d.y - 6)
        .attr("x", d => d.x + 6) // text
        .attr("y", d => d.y - 6);
}

function drag(simulation) {
    function dragstarted(event, d) {
        clearTimeout(hoverTimeout);
        tooltip.style("display", "none");

        isDragActive = true;
        if (!event.active) simulation.alphaTarget(0.01).restart();
        event.subject.fx = event.subject.x;
        event.subject.fy = event.subject.y;
    }

    function dragged(event) {
        event.subject.fx = event.x;
        event.subject.fy = event.y;
    }

    function dragended(event) {
        isDragActive = false;
        if (!event.active) simulation.alphaTarget(0);
        event.subject.fx = null;
        event.subject.fy = null;
    }

    return d3.drag()
        .on("start", dragstarted)
        .on("drag", dragged)
        .on("end", dragended);
}
})


// // Good enough
function rectangleCollide() {
    let nodes;
    let strength;
    
    function force() {
      for (let i = 0, n = nodes.length; i < n; ++i) {
        const nodeI = nodes[i];
        for (let j = i + 1; j < n; ++j) {
          const nodeJ = nodes[j];
          const dx = nodeJ.x - nodeI.x;
          const dy = nodeJ.y - nodeI.y;

          const halfWidthI = nodeI.width / 2;
          const halfWidthJ = nodeJ.width / 2;
          const halfHeightI = nodeI.height / 2;
          const halfHeightJ = nodeJ.height / 2;

          // Effective width and height when considering both nodes
          const width = halfWidthI + halfWidthJ;
          const height = halfHeightI + halfHeightJ;

          const absDx = Math.abs(dx);
          const absDy = Math.abs(dy);
          if (absDx < width && absDy < height) {
            const overlapX = width - absDx;
            const overlapY = height - absDy;

            if (overlapX < overlapY) {
              if (dx > 0) {
                nodeJ.x += overlapX / 2;
                nodeI.x -= overlapX / 2;
              } else {
                nodeJ.x -= overlapX / 2;
                nodeI.x += overlapX / 2;
              }
            } else {
              if (dy > 0) {
                nodeJ.y += overlapY / 2;
                nodeI.y -= overlapY / 2;
              } else {
                nodeJ.y -= overlapY / 2;
                nodeI.y += overlapY / 2;
              }
            }
          }
        }
      }
    }
  
    force.initialize = function(_) {
      nodes = _;
    };

    force.strength = function (_) {
        return (arguments.length ? (strength = +_, force) : strength)
    }

    force.strength = function (_) {
        return (arguments.length ? (strength = +_, force) : strength)
    }
    
    return force;
  }