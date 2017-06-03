var page = require('webpage').create(),
  system = require('system'),
  t, address;

if (system.args.length === 1) {
  console.log('Usage: screenshot.js <some URL>');
  phantom.exit();
}
page.open(system.args[1], function(status) {
  if(status === "success") {
    page.render('/dev/stdout', {format: 'png'});
  }
  phantom.exit();
});
