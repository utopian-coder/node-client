const net = require("net");
const fs = require("fs");

const HOST = "localhost";
const PORT = 3000;

const CALL_TYPE_STREAM_ALL = 1;
const CALL_TYPE_RESEND_PACKET = 2;

// Store received packets and track missing sequences
let receivedPackets = [];
let missingSequences = [];
let bufferCollector = Buffer.alloc(0);
let maxSequence = 0;
let isStreamComplete = false;
let retryAttempts = 0;
const MAX_RETRIES = 5;

const client = new net.Socket();

client.connect(PORT, HOST, () => {
  console.log("Connected to server");
  requestAllPackets();
});

function requestAllPackets() {
  const buffer = Buffer.alloc(2);
  buffer.writeUInt8(CALL_TYPE_STREAM_ALL, 0);
  buffer.writeUInt8(0, 1);
  client.write(buffer);
}

function requestPacket(sequenceNumber) {
  const buffer = Buffer.alloc(2);
  buffer.writeUInt8(CALL_TYPE_RESEND_PACKET, 0);
  buffer.writeUInt8(sequenceNumber, 1);
  client.write(buffer);
}

client.on("data", (data) => {
  bufferCollector = Buffer.concat([bufferCollector, data]);
  parsePackets();
  if (isStreamComplete) {
    checkForMissingPackets();
  }
});

function parsePackets() {
  while (bufferCollector.length >= 17) {
    const packet = {
      symbol: bufferCollector.toString("ascii", 0, 4).trim(),
      buysellindicator: bufferCollector.toString("ascii", 4, 5),
      quantity: bufferCollector.readInt32BE(5),
      price: bufferCollector.readInt32BE(9),
      packetSequence: bufferCollector.readInt32BE(13),
    };
    receivedPackets.push(packet);
    bufferCollector = bufferCollector.slice(17);
    console.log(`Received packet: ${JSON.stringify(packet)}`);

    if (packet.packetSequence > maxSequence) {
      maxSequence = packet.packetSequence;
    }
  }

  if (bufferCollector.length === 0) {
    isStreamComplete = true;
  }
}

function checkForMissingPackets() {
  const sequences = receivedPackets.map((packet) => packet.packetSequence);

  missingSequences = [];
  for (let i = 1; i <= maxSequence; i++) {
    if (!sequences.includes(i)) {
      missingSequences.push(i);
    }
  }

  if (missingSequences.length > 0) {
    console.log(`Requesting missing packets: ${missingSequences}`);
    missingSequences.forEach((seq) => requestPacket(seq));
  } else {
    generateJSONFile();
    client.end();
  }
}

function generateJSONFile() {
  const sortedPackets = receivedPackets.sort(
    (a, b) => a.packetSequence - b.packetSequence
  );
  const jsonOutput = JSON.stringify(sortedPackets, null, 2);
  fs.writeFileSync("output.json", jsonOutput);
  console.log("JSON file generated with all packets");
}

client.on("close", () => {
  console.log("Connection closed");

  if (retryAttempts < MAX_RETRIES && missingSequences.length > 0) {
    retryAttempts++;

    console.log(
      `Retrying missing packet requests... (Attempt ${retryAttempts})`
    );

    setTimeout(() => {
      client.connect(PORT, HOST, () => {
        console.log("Reconnected to server");
        missingSequences.forEach((seq) => requestPacket(seq));
      });
    }, 1000 * retryAttempts);
  } else {
    if (retryAttempts >= MAX_RETRIES) {
      console.error("Max retry attempts reached. Exiting...");
    } else {
      console.log("All packets received successfully.");
    }
  }
});

client.on("error", (err) => {
  console.error("Error:", err.message);
  client.end();
});
