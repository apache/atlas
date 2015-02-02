'use strict';

module.exports = function(app) {

    app.get('/', function(req, res) {
        console.log('appConfig', app.config);
        res.render('index', {
            renderErrors: {}, //req.flash('error')
            app: app.config.app
        });
    });
};
