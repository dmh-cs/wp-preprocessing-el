var dumpster = require('dumpster-dive');

var dbname = 'enwiki';
var dump_path = './enwiki-20180520-pages-articles.xml';
console.log('Loading wikipedia dump at', dump_path, 'to mongodb db', dbname);
dumpster({file: dump_path,
          db: dbname,
          depth: false,
          skip_redirects: true,
          skip_disambig: false,
          custom: function(doc) {
	      return {
		  title: doc.title(),
		  categories: doc.categories(),
                  isDisambiguation: doc.isDisambiguation(),
                  sections: doc.sections().map(i => i.json()),
                  plaintext: doc.plaintext()
	      }
	  }
         });
