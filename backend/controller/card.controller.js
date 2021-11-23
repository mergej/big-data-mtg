const express = require('express');
const router = express.Router();
const cardService = require('../services/card.service');


/**
 * GET all cards
 */
router.get('/', (req, res) => {
    cardService.getAllCards(req.query.page, req.query.pageSize).then((cards) => {
        res.status(200);
        res.json(cards.rows);
    }).catch((err) => {
        res.status(500);
        res.json(err);
    });
});

/**
 * GET card by multiverseId
 */
router.get('/:multiverseId', (req, res) => {
    cardService.getCardByMultiverseId(req.params.multiverseId).then((card) => {
        res.status(200);
        res.json(card.rows[0]);
    }).catch((err) => {
        res.status(500);
        res.json(err);
    });
});

/**
 * FET Card by Id
 */
router.get('/id/:id', (req, res) => {
    cardService.getCardById(req.params.id).then((card) => {
        res.status(200);
        res.json(card.rows[0]);
    }).catch((err) => {
        res.status(500);
        res.json(err);
    });
});

/**
 * GET cards by name substring
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
 * GET cards by artist substring
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
 *  get cards by substring multiverse_id
 */
router.get('/multiverseId/:multiverseId', (req, res) => {
    cardService.getCardsByMultiverseIdSubstring(req.params.multiverseId).then((cards) => {
        res.status(200);
        res.json(cards.rows);
    }).catch((err) => {
        res.status(500);
        res.json(err);
    });
});

module.exports = router;