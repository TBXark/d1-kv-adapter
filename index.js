/**
 * Converts an ArrayBuffer to a base64 string.
 *
 * @param {ArrayBuffer} buffer - The buffer to convert.
 * @return {string} The base64 string representation of the buffer.
 */
function arrayBufferToBase64(buffer) {
    let binary = '';
    const bytes = new Uint8Array(buffer);
    const len = bytes.byteLength;
    for (let i = 0; i < len; i++) {
      binary += String.fromCharCode(bytes[i]);
    }
    return btoa(binary);
  }
  
  /**
   * Convert a base64 string to an ArrayBuffer.
   *
   * @param {string} base64 - The base64 string to convert.
   * @return {ArrayBuffer} - The converted ArrayBuffer.
   */
  function base64ToArrayBuffer(base64) {
    const binaryString = atob(base64);
    const len = binaryString.length;
    const bytes = new Uint8Array(len);
    for (let i = 0; i < len; i++) {
      bytes[i] = binaryString.charCodeAt(i);
    }
    return bytes.buffer;
  }
  
  export class D1KVAdapter {
    /**
     * Constructor for the class.
     *
     * @param {object} d1
     * @param {string} table
     */
    constructor(d1, table) {
      this.d1 = d1;
      this.table = table || 'KV';
      this.getStmt = d1.prepare(`SELECT * FROM ${this.table} WHERE key = ?`);
      this.putStmt = d1.prepare(`INSERT OR REPLACE INTO ${this.table} (key, value, expires) VALUES (?, ?, ?)`);
      this.deleteStmt = d1.prepare(`DELETE FROM ${this.table} WHERE key = ?`);
    }
  
    /**
       *
       * @return {Promise<void>}
       */
    async init() {
      await this.d1.prepare(`CREATE TABLE IF NOT EXISTS ${this.table} (key TEXT PRIMARY KEY, value TEXT, expires INTEGER)`).run();
    }
  
    /**
       * Retrieves the value associated with the specified key from the cache.
       *
       * @param {string} key - The key for which to retrieve the value.
       * @param {object} info - Additional information about the retrieval.
       * @return {Promise} The value associated with the key, or null if the key does not exist or has expired.
       */
    async get(key, info) {
      const raw = await this.getStmt.bind(key).first();
      if (raw) {
        if (raw.expires === -1) {
          return raw.value;
        } else if (raw.expires > Date.now()) {
          switch (info?.type || 'string') {
            case 'string':
              return raw.value;
            case 'json':
              return JSON.parse(raw.value);
            case 'arrayBuffer':
              return base64ToArrayBuffer(raw.value);
            case 'stream':
              return new ReadableStream({
                start(controller) {
                  controller.enqueue(raw.value);
                  controller.close();
                },
              });
  
            default:
              return raw.value;
          }
        } else {
          this.delete(key).catch(console.error.bind(console));
          return null;
        }
      }
      return raw;
    }
  
    /**
       * Asynchronously puts a key-value pair in the cache.
       *
       * @param {string} key - The key for the cache entry.
       * @param {any} value - The value for the cache entry.
       * @param {object} info - Additional information for the cache entry.
       * @return {Promise} - The result of the put operation.
       */
    async put(key, value, info) {
      let expires = -1;
      if (info && info.expiration) {
        expires = Math.round(info.expiration);
      } else if (info && info.expirationTtl) {
        expires = Math.round(Date.now() + info.expirationTtl * 1000);
      }
      if (value instanceof ArrayBuffer) {
          value = arrayBufferToBase64(value);
      } else {
          switch (typeof value) {
              case 'string':
                break;
              case 'object':
              case 'number':
                value = JSON.stringify(value);
                break;
              default:
                throw new Error('Unsupported value type');
            }
      }
     
      const raw = await this.putStmt.bind(key, value, expires).run();
      return raw;
    }
  
    /**
       * Deletes a record from the database with the given key.
       *
       * @param {string} key - The key of the record to be deleted.
       * @return {Promise} The raw result of the delete operation.
       */
    async delete(key) {
      const raw = await this.deleteStmt.bind(key).run();
      return raw;
    }
  }
  

export default {
    async fetch(request, env) {
        try {
            const db = new D1KVAdapter(env.DB);
            await db.init();
            const url = new URL(request.url);
            const path = url.pathname;
            const key = url.searchParams.get('key');
            switch (path) {
                case '/kv':
                    return new Response(JSON.stringify(await db.get(key)));
                case '/kv/put':
                    const value = url.searchParams.get('value');
                    return new Response(JSON.stringify(await db.put(key, value)));
                case '/kv/delete':
                    return new Response(JSON.stringify(await db.delete(key)));
                default: 
                    return new Response('Not found', { status: 404 });
            }
        } catch (e) {
            return new Response(e.message, { status: 500 });
        }
    },
};