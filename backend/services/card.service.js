const db = require('../models/db.model');

function getAllCards(page, pageSize) {
    page = Number(page);
    pageSize = Number(pageSize);
    bottom = (page - 1) * pageSize + 1;
    top = bottom + pageSize - 1;

    return new Promise((resolve, reject) => {
        const statement = `SELECT * FROM mtg_cards_jakob WHERE id BETWEEN ${bottom} AND ${top};`;

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

function getCardsByMultiverseIdSubstring(multiverseId) {
    return new Promise((resolve, reject) => {
        const statement = `SELECT * FROM mtg_cards_jakob WHERE multiverseId LIKE '%${multiverseId}%';`;

        db.sqlQuery(statement).then((result) => {
            if (result.status == db.StatusEnum.SUCCESS) {
                resolve(result);
            } else {
                reject(result);
            }
        });
    });
}

function getCardByMultiverseId(multiverseId) {
    return new Promise((resolve, reject) => {
        const statement = `SELECT * FROM mtg_cards_jakob WHERE multiverseId = '${multiverseId}';`;

        db.sqlQuery(statement).then((result) => {
            if (result.status == db.StatusEnum.SUCCESS) {
                resolve(result);
            } else {
                reject(result);
            }
        });
    });
}

function getCardById(id) {
    return new Promise((resolve, reject) => {
        const statement = `SELECT * FROM mtg_cards_jakob WHERE id = '${id}';`;

        db.sqlQuery(statement).then((result) => {
            if (result.status == db.StatusEnum.SUCCESS) {
                resolve(result);
            } else {
                reject(result);
            }
        });
    });
}

module.exports = {
    getAllCards,
    getCardByMultiverseId,
    getCardById,
    getCardsByName,
    getCardsByArtist,
    getCardsByMultiverseIdSubstring,
};