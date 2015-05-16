var fs      = require('fs');
var path    = require('path');
var debug   = require('debug')('scripto');
var crypto  = require('crypto');

function Scripto (redisClient) {

    var scripts = {};
    var scriptShas = this._scriptShas = {};

    this.load = function load(scriptObject) {
        mergeObjects(scripts, scriptObject);
        lazyLoadScriptsIntoRedis(redisClient, scriptObject, afterShasLoaded);
    };

    this.loadFromFile = function loadFromFile(name, filepath) {
        var loadedScripts = {};
        loadedScripts[name] = fs.readFileSync(filepath, 'utf8');
        this.load(loadedScripts);
    };

    this.loadFromDir = function loadFromDir(scriptsDir) {
        var loadedScripts = loadScriptsFromDir(scriptsDir);
        this.load(loadedScripts);
    };


    this.run = function run(scriptName, keys, args, callback) {
        if(scripts[scriptName]) {
            if(scriptShas[scriptName]) {
                var sha = scriptShas[scriptName];
                evalShaScript(redisClient, sha, keys, args, function(err, result) {

                    if(err && isScriptNotLoadedError(err)) {
                        /**
                         * The script was once loaded but is no longer loaded.
                         * We may have switched redis instances using sentinel
                         */
                        loadScriptIntoRedis(redisClient, scripts[scriptName], function(err, sha) {
                            if(err) return callback(err);

                            scriptShas[scriptName] = sha;
                            evalShaScript(redisClient, sha, keys, args, callback);
                        });

                        return;
                    }

                    callback(err, result);
                });
            } else {
                var script = scripts[scriptName];
                loadScriptIntoRedis(redisClient, script, function(err, sha) {
                    if(err) return callback(err);

                    scriptShas[scriptName] = sha;
                    evalShaScript(redisClient, sha, keys, args, callback);
                });
            }
        } else {
            callback(new Error('NO_SUCH_SCRIPT'));
        }
    };

    this.eval = this.run;

    this.evalSha = function evalSha(scriptName, keys, args, callback) {

        if(scriptShas[scriptName]) {
            var sha = scriptShas[scriptName];
            evalShaScript(redisClient, sha, keys, args, callback);
        } else {
            callback(new Error('NO_SUCH_SCRIPT_SHA'));
        }
    };

    //load scripts into redis in every time it connects to it
    redisClient.on('connect', function() {
        debug('loading scripts into redis again, after-reconnect');
        lazyLoadScriptsIntoRedis(redisClient, scripts, afterShasLoaded);
    });

    //reset shas after error occured
    redisClient.on('error', function(err) {
        var errorMessage = (err)? err.toString() : "";
        debug('resetting scriptShas due to redis connection error: ' + errorMessage);
        scriptShas = {};
    });

    function afterShasLoaded(err, shas) {

        if(err) {
            debug('scripts loading failed due to redis command error: ' + err.toString());
        } else {
            debug('loaded scriptShas');
            mergeObjects(scriptShas, shas);
        }
    }

    function mergeObjects (obj1, obj2) {

        for(var key in obj2) {
            obj1[key] = obj2[key];
        }
    }

}

module.exports = Scripto;

function loadScriptsFromDir(scriptsDir) {

    var names = fs.readdirSync(scriptsDir);
    var scripts = {};

    names.forEach(function(name) {
        var filename = path.resolve(scriptsDir, name);
        var stat = fs.statSync(filename);

        if (stat.isFile()) {
            var key = path.basename(filename, '.lua');
            if (key.charAt(0) === '.') return; // Ignore .dotfiles
            scripts[key] = fs.readFileSync(filename, 'utf8');
        }
    });

    return scripts;
}

function lazyLoadScriptsIntoRedis (redisClient, scripts, callback) {
    var cnt = 0;
    var keys = Object.keys(scripts);
    var shas = {};

    (function doLoad() {

        if(cnt < keys.length) {
            var key = keys[cnt++];

            lazyLoadScriptIntoRedis(redisClient, scripts[key], function(err, sha) {

                if(err) {
                    callback(err);
                } else {
                    shas[key] = sha;
                    doLoad();
                }
            });
        } else {
            callback(null, shas);
        }

    })();
}

function isScriptNotLoadedError(err) {
    return err.message && err.message.indexOf('NOSCRIPT No matching script') === 0;
}

function lazyLoadScriptIntoRedis(redisClient, script, callback) {
    var hasher = crypto.createHash('sha1');
    hasher.update(script);
    var shaSum = hasher.digest('hex');
    redisClient.sendCommand('script', ['exists', shaSum], function(err, results) {
        var scriptExists = !err && results && results[0] === 1;
        if (scriptExists) return callback(null, shaSum);

        loadScriptIntoRedis(redisClient, script, callback);
    });
}

function loadScriptIntoRedis(redisClient, script, callback) {
    redisClient.sendCommand('script', ['load', script], callback);
}

function evalScript(redisClient, script, keys, args, callback) {
    var keysLength= keys.length || 0;
    var scriptArgs = [script, keysLength].concat(keys, args);
    redisClient.sendCommand('eval', args, callback);
}

function evalShaScript(redisClient, sha, keys, args, callback) {
    var keysLength= keys.length || 0;
    var scriptArgs = [sha, keysLength].concat(keys, args);
    redisClient.sendCommand('evalsha', scriptArgs, callback);
}
