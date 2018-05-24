var dumpster = require('dumpster-dive');

dumpster({file:'./afwiki-latest-pages-articles.xml',
          db: 'afwiki',
          depth: false,
          plaintext: true,
          infoboxes: false,
          citations: false,
          sections: true,
          images: false,
          categories: true,
          links: true
         });
