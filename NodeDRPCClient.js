/* THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

This content is released under the MIT License.
http://opensource.org/licenses/MIT
*/


var thrift = require('./thrift_module/thrift/lib/thrift'),
  connection,
  client;

var DistributedRPC = require('./lib/DistributedRPC.js'),
    ttypes = require('./lib/storm_types.js');

function NodeDRPCClient (hostName, portNo, timeout) {

 if (!hostName || !portNo) {
    throw new Error("NodeDRPCClient initialization error ! Hostname and Port number must be specified.");

 }

 if (typeof(hostName) !== 'string') {
    throw new Error("NodeDRPCClient initialization error ! Hostname must be a string.");
 }


 if (typeof(portNo) !==  'number') {
    throw new Error("NodeDRPCClient initialization error ! PortNo must be a integer.");
 }

 if ( timeout != null && typeof(timeout) !==  'number') {
    throw new Error("NodeDRPCClient initialization error ! Timeout must be a integer.");
 }

 this.hostName = hostName;
 this.portNo = portNo;
 this.timeout = timeout;
}

NodeDRPCClient.prototype = {};


NodeDRPCClient.prototype.execute = function(drpcFunction, drpcFunctionParam, callback ) {
  if (typeof(callback) !== 'function') {
    throw new Error("NodeDRPCClient initialization error ! Callback must be a function.");
  }

  if (!connection) {
    connection = thrift.createConnection(this.hostName, this.portNo, this.timeout);
    client = thrift.createClient(DistributedRPC, connection);
    connection.on('error', function(err) {
      callback(err);
      connection.end();
      connection = null;
    });

    connection.on('close', function() {
      connection = null;
    });
  }

  client.execute(drpcFunction, drpcFunctionParam, callback);
}

module.exports = NodeDRPCClient;
