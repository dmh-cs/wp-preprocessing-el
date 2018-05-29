var dumpster = require('dumpster-dive');

var dbname = 'afwiki';
var dump_path = './afwiki-latest-pages-articles.xml';
console.log('Loading wikipedia dump at', dump_path, 'to mongodb db', dbname);
dumpster({file: dump_path,
          db: dbname,
          depth: false,
          plaintext: true,
          infoboxes: false,
          citations: false,
          sections: true,
          images: false,
          categories: true,
          links: true
         });
