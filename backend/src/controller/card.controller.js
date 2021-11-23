const express = require('express');
const router = express.Router();
const cardService = require('../services/card.service');

/**
 * get all cards
 */
router.get('/', (req, res) => {
    cardService.getAllCards().then((cards) => {
        res.status(200);
        res.json(cards.rows);
    }).catch((err) => {
        res.status(500);
        res.json(err);
    });
});

/**
 * get cards by substring name
 */
router.get('/name/:name', (req, res) => {
    cardService.getCardsByName(req.params.name).then((cards) => {
        res.status(200);
        res.json(cards.rows);
    }).catch((err) => {
        res.status(500);
        res.json(err);
    });
});

/**
 * get cards by substring artist
 */
router.get('/artist/:artist', (req, res) => {
    cardService.getCardsByArtist(req.params.artist).then((cards) => {
        res.status(200);
        res.json(cards.rows);
    }).catch((err) => {
        res.status(500);
        res.json(err);
    });
});

/**
 * get cards by substring multiverseId
 */
router.get('/multiverseId/:multiverseId', (req, res) => {
    cardService.getCardsByMultiverseId(req.params.multiverseId).then((cards) => {
        res.status(200);
        res.json(cards.rows);
    }).catch((err) => {
        res.status(500);
        res.json(err);
    });
});

module.exports = router;