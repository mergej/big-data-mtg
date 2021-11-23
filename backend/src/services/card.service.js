const db = require('../models/mysql');


function getAllCards() {
    return new Promise((resolve, reject) => {
        const statement = 'SELECT * FROM mtg_cards_jakob LIMIT 100;';

        db.sqlQuery(statement).then((result) => {
            if (result.status == db.StatusEnum.SUCCESS) {
                resolve(result);
            } else {
                reject(result);
            }
        });
    });
}

function getCardsByName(name) {
    return new Promise((resolve, reject) => {
        const statement = `SELECT * FROM mtg_cards_jakob WHERE name LIKE '%${name}%';`;

        db.sqlQuery(statement).then((result) => {
            if (result.status == db.StatusEnum.SUCCESS) {
                resolve(result);
            } else {
                reject(result);
            }
        });
    });
}

function getCardsByArtist(artist) {
    return new Promise((resolve, reject) => {
        const statement = `SELECT * FROM mtg_cards_jakob WHERE artist LIKE '%${artist}%';`;

        db.sqlQuery(statement).then((result) => {
            if (result.status == db.StatusEnum.SUCCESS) {
                resolve(result);
            } else {
                reject(result);
            }
        });
    });
}

function getCardsByMultiverseId(multiverseId) {
    return new Promise((resolve, reject) => {
        const statement = `SELECT * FROM mtg_cards_jakob WHERE multiverseId LIKE '%${multiverseId}%';`;

        db.sqlQuery(statement).then((result) => {
            if (result.status == db.StatusEnum.SUCCESS) {
                console.log(result);
                resolve(result);
            } else {
                reject(result);
            }
        });
    });
}


module.exports = {
    getAllCards,
    getCardsByName,
    getCardsByArtist,
    getCardsByMultiverseId
};