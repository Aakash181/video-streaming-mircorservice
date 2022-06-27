const express = require("express");
const http = require("http");
const fs = require("fs");
const mongodb = require("mongodb");
const app = express();

// Throws an error if the any required environment variables are missing.

if (!process.env.PORT) {
    throw new Error("Please specify the port number for the HTTP server with the environment variable PORT.");
}

if (!process.env.VIDEO_STORAGE_HOST) {
    throw new Error("Please specify the host name for the video storage microservice in variable VIDEO_STORAGE_HOST.");
}

if (!process.env.VIDEO_STORAGE_PORT) {
    throw new Error("Please specify the port number for the video storage microservice in variable VIDEO_STORAGE_PORT.");
}

//
// Extracts environment variables to globals for convenience.
//
const PORT = process.env.PORT;
const VIDEO_STORAGE_HOST = process.env.VIDEO_STORAGE_HOST;
const VIDEO_STORAGE_PORT = parseInt(process.env.VIDEO_STORAGE_PORT);
const DBHOST = process.env.DBHOST;
const DBNAME = process.env.DBNAME;
console.log(`Forwarding video requests to ${VIDEO_STORAGE_HOST}:${VIDEO_STORAGE_PORT}.`);
//-------------------------------------------------------------------------------------------------------------------


function sendViewedMessage(videoPath) {
    const postOptions = { // Options to the HTTP POST request.
        method: "POST", // Sets the request method as POST.
        headers: {
            "Content-Type": "application/json", // Sets the content type for the request's body.
        },
    };

    const requestBody = { // Body of the HTTP POST request.
        videoPath: videoPath 
    };

    const req = http.request( // Send the "viewed" message to the history microservice.
        "http://history/viewed",
        postOptions
    );

    req.on("close", () => {
        console.log("Sent 'viewed' message to history microservice.");
    });

    req.on("error", (err) => {
        console.error("Failed to send 'viewed' message!");
        console.error(err && err.stack || err);
    });

    req.write(JSON.stringify(requestBody)); // Write the body to the request.
    req.end(); // End the request.
}


function setupHandlers(app) {
    app.get("/video", (req, res) => { // Route for streaming video.

        const videoPath = `${process.env.videoPath1}`;
        fs.stat(videoPath, (err, stats) => {
            if (err) {
                console.error("An error occurred ");
                res.sendStatus(500);
                return;
            }
    
            res.writeHead(200, {
                "Content-Length": stats.size,
                "Content-Type": "video/mp4"
            });
    
            fs.createReadStream(videoPath).pipe(res);

            sendViewedMessage(videoPath); // Send message to "history" microservice that this video has been "viewed".
        });
    });
}

function startHttpServer() {
    return new Promise(resolve => { // Wrap in a promise so we can be notified when the server has started.
        const app1 = express();
        setupHandlers(app1);
        const port1 = process.env.port1 || 3006 // && parseInt(process.env.PORT) || 3000;
//        app1.listen(port1, () => {
//            resolve();
//        });
    });
}

function main2() {
    return startHttpServer();
}

//---------------------------------------------------------------------------------------------------------------------

function main() {
    return mongodb.MongoClient.connect(DBHOST) // Connect to the database.
        .then(client => {
            const db = client.db(DBNAME);
            const videosCollection = db.collection("videos");
            console.log("connected to database ");
            app.get("/video", (req, res) => {
                //console.log("this function is runing");
                const videoId = new mongodb.ObjectId(req.query.id);
                videosCollection.findOne({ _id: videoId })
                    .then(videos => {
                        if (!videos) {
                            res.sendStatus(404);
                            return;
                        }
                       // console.log("hi my name is aakash");
                        console.log(`Translated id ${videoId} to path ${videos.videoPath}.`);
        
                        const forwardRequest = http.request( // Forward the request to the video storage microservice.
                            {
                                host: VIDEO_STORAGE_HOST,
                                port: VIDEO_STORAGE_PORT,
                                path:`/video?path=${videos.videoPath}`, // Video path now retrieved from the database.
                                method: 'GET',
                                headers: req.headers
                            }, 
                            forwardResponse => {
                                res.writeHeader(forwardResponse.statusCode, forwardResponse.headers);
                                forwardResponse.pipe(res);
                            }
                        );
                        
                        req.pipe(forwardRequest);
                        const videopath1=process.env.videoPath1 || `${videos.videoPath}`
                        main2()
                        .then(() => console.log("sending video path to history Microservice"))
                        .catch(err => {
                            console.log("Microservice failed to start.");
                            console.error(err && err.stack || err);
                        });
                        
                    })
                    .catch(err => {
                        console.error("Database query failed.");
                        console.error(err && err.stack || err);
                        res.sendStatus(500);
                    });
            });

            //
            // Starts the HTTP server.
            //
            app.listen(PORT, () => {
                console.log(`Microservice listening, please load the data file db-fixture/videos.json into your database before testing this microservice.`);
            });
        });
}

main()
    .then(() => console.log("Microservice online."))
    .catch(err => {
        console.error("Microservice failed to start.");
        console.error(err && err.stack || err);
    });
