/**
 * Cryptonote Node.JS Pool
 * https://github.com/dvandal/cryptonote-nodejs-pool
 *
 * Payments processor
 **/

// Load required modules
let fs = require('fs');
let async = require('async');

let apiInterfaces = require('./apiInterfaces.js')(config.daemon, config.wallet, config.api);
let notifications = require('./notifications.js');
let utils = require('./utils.js');

// Initialize log system
let logSystem = 'payments';
require('./exceptionWriter.js')(logSystem);

/**
 * Run payments processor
 **/

log('info', logSystem, 'Started');

if (!config.poolServer.paymentId) config.poolServer.paymentId = {};
if (!config.poolServer.paymentId.addressSeparator) config.poolServer.paymentId.addressSeparator = "+";
if (!config.payments.priority) config.payments.priority = 0;

function runInterval () {
	async.waterfall([

		// Get worker keys
		function (callback) {
			redisClient.keys(config.coin + ':workers:*', function (error, result) {
				if (error) {
					log('error', logSystem, 'Error trying to get worker balances from redis %j', [error]);
					callback(true);
					return;
				}
				callback(null, result);
			});
		},

		// Get worker balances
		function (keys, callback) {
		        let redisCommands = [];
			keys.forEach(function (k) {
			    if (config.coin == "haven") {
				for (let asset in config.payments.assets) {
				    if (asset === "XHV") {
					redisCommands.push(['hget', k, 'balance']);
				    } else {
					redisCommands.push(['hget', k, 'balance-'+asset]);
				    }
				}
			    } else {
				redisCommands.push(['hget', k, 'balance']);
			    }
			});
		        //log('error', logSystem, 'redisCommands = %j', [redisCommands]);
			redisClient.multi(redisCommands)
				.exec(function (error, replies) {
				    if (error) {
					log('error', logSystem, 'Error with getting balances from redis %j', [error]);
					callback(true);
					return;
				    }

				    let balances = {};
				    if (config.coin == "haven") {
					let idx = 0;
					for (let i = 0; i < replies.length; idx++) {
					    let parts = keys[idx].split(':');
					    let workerId = parts[parts.length - 1];
					    balances[workerId] = {};
					    for (let asset in config.payments.assets) {
						balances[workerId][asset] = parseInt(replies[i++]) || 0;
					    }
					}
					//log('error', logSystem, 'BALANCES = %j, REPLIES = %j', [balances, replies]);
				    } else {
					for (let i = 0; i < replies.length; i++) {
					    let parts = keys[i].split(':');
					    let workerId = parts[parts.length - 1];
					    
					    balances[workerId] = parseInt(replies[i]) || 0;
					}
				    }
				    callback(null, keys, balances);
				});
		},

		// Get worker minimum payout
		function (keys, balances, callback) {
		        let redisCommands = [];
		        keys.forEach(function (k) {
			    if (config.coin == "haven") {
				for (let asset in config.payments.assets) {
				    if (asset === "XHV") {
					redisCommands.push(['hget', k, 'minPayoutLevel']);
				    } else {
					redisCommands.push(['hget', k, 'minPayoutLevel-'+asset]);
				    }
				}
			    } else {
				redisCommands.push(['hget', k, 'minPayoutLevel']);
			    }
			});
			//log('error', logSystem, 'redisCommands = %j', [redisCommands]);
			redisClient.multi(redisCommands)
				.exec(function (error, replies) {
					if (error) {
						log('error', logSystem, 'Error with getting minimum payout from redis %j', [error]);
						callback(true);
						return;
					}

				        let minPayoutLevel = {};
				        let idx = 0;
					for (let i = 0; i < replies.length; idx++) {
						let parts = keys[idx].split(':');
						let workerId = parts[parts.length - 1];

					    if (config.coin == "haven") {
						minPayoutLevel[workerId] = {};
						Object.keys(config.payments.assets).forEach(function(asset) {
						    let minLevel = config.payments.assets[asset].minPayment;
						    let maxLevel = config.payments.assets[asset].maxPayment;
						    let defaultLevel = minLevel;

						    let payoutLevel = parseInt(replies[i++]) || minLevel;
						    if (payoutLevel < minLevel) payoutLevel = minLevel;
						    if (maxLevel && payoutLevel > maxLevel) payoutLevel = maxLevel;
						    minPayoutLevel[workerId][asset] = payoutLevel;
						    
						    if (payoutLevel !== defaultLevel) {
							log('info', logSystem, 'Using payout level of %s for %s (default: %s)', [utils.getReadableCoins(minPayoutLevel[workerId][asset]), workerId, utils.getReadableCoins(defaultLevel)]);
						    }
						});
					    } else {
						let minLevel = config.payments.assets.minPayment;
						let maxLevel = config.payments.assets.maxPayment;
						let defaultLevel = minLevel;

						let payoutLevel = parseInt(replies[i]) || minLevel;
						if (payoutLevel < minLevel) payoutLevel = minLevel;
						if (maxLevel && payoutLevel > maxLevel) payoutLevel = maxLevel;
						minPayoutLevel[workerId] = payoutLevel;

						if (payoutLevel !== defaultLevel) {
							log('info', logSystem, 'Using payout level of %s for %s (default: %s)', [utils.getReadableCoins(minPayoutLevel[workerId]), workerId, utils.getReadableCoins(defaultLevel)]);
						}
					    }
					}
					callback(null, balances, minPayoutLevel);
				});
		},

		// Filter workers under balance threshold for payment
		function (balances, minPayoutLevel, callback) {
			let payments = {};
			for (let worker in balances) {
			    if (config.coin == "haven") {
				payments[worker] = {};
				for (let asset in balances[worker]) {
				    
				    let balance = balances[worker][asset];
				    if (balance >= minPayoutLevel[worker][asset]) {
					let remainder = balance % config.payments.assets[asset].denomination;
					let payout = balance - remainder;
					
					if (config.payments.assets[asset].dynamicTransferFee && config.payments.assets[asset].minerPayFee) {
					    payout -= config.payments.assets[asset].transferFee;
					}
					if (payout < 0) continue;
					
					payments[worker][asset] = payout;
				    }
				}
				log('error', logSystem, 'PAYMENTS = %j', [payments]);
				
			    } else {
				let balance = balances[worker];
				if (balance >= minPayoutLevel[worker]) {
					let remainder = balance % config.payments.assets.denomination;
					let payout = balance - remainder;

					if (config.payments.assets.dynamicTransferFee && config.payments.assets.minerPayFee) {
						payout -= config.payments.assets.transferFee;
					}
					if (payout < 0) continue;

					payments[worker] = payout;
				}
			    }
			}

		        log('error', logSystem, 'PAYMENTS ADJUSTED = %j', [payments]);
		    
			if (Object.keys(payments)
				.length === 0) {
				log('info', logSystem, 'No workers\' balances reached the minimum payment threshold');
				callback(true);
				return;
			}

			let transferCommands = [];
			let addresses = 0;
			let commandAmount = 0;
			let commandIndex = 0;
		        let ringSize = (config.payments.ringSize ? config.payments.ringSize : config.payments.mixin);

		        for (let worker in payments) {
			    if (config.coin == "haven") {
				for (let asset in payments[worker]) {
				    let amount = parseInt(payments[worker][asset]);
				    if (config.payments.assets[asset].maxTransactionAmount && amount + commandAmount > config.payments.assets[asset].maxTransactionAmount) {
					amount = config.payments.assets[asset].maxTransactionAmount - commandAmount;
				    }
				    
				    let address = worker;
				    let payment_id = null;
				    
				    let with_payment_id = false;
				    
				    let addr = address.split(config.poolServer.paymentId.addressSeparator);
				    if ((addr.length === 1 && utils.isIntegratedAddress(address)) || addr.length >= 2) {
					with_payment_id = true;
					if (addr.length >= 2) {
					    address = addr[0];
					    payment_id = addr[1];
					    payment_id = payment_id.replace(/[^A-Za-z0-9]/g, '');
					    if (payment_id.length !== 16 && payment_id.length !== 64) {
						with_payment_id = false;
						payment_id = null;
					    }
					}
					if (addresses > 0) {
					    commandIndex++;
					    addresses = 0;
					    commandAmount = 0;
					}
				    }

				    if (config.poolServer.fixedDiff && config.poolServer.fixedDiff.enabled) {
					addr = address.split(config.poolServer.fixedDiff.addressSeparator);
					if (addr.length >= 2) address = addr[0];
				    }

				    if (!transferCommands[commandIndex]) {
					transferCommands[commandIndex] = {
					    redis: [],
					    amount: 0,
					    rpc: {
						destinations: [],
						fee: config.payments.assets[asset].transferFee,
						priority: config.payments.assets[asset].priority,
						unlock_time: 0,
						asset_type: asset,
						method: (asset == "XHV" ? "transfer_split" : asset == "XUSD" ? "offshore_transfer" : "xasset_transfer")
					    }
					};
					if (config.payments.assets[asset].ringSize)
					    transferCommands[commandIndex].rpc.ring_size = ringSize;
					else
					    transferCommands[commandIndex].rpc.mixin = ringSize;
				    }

				    transferCommands[commandIndex].rpc.destinations.push({
					amount: amount,
					address: address
				    });
				    if (payment_id) transferCommands[commandIndex].rpc.payment_id = payment_id;

				    if (asset == "XHV") {
					transferCommands[commandIndex].redis.push(['hincrby', config.coin + ':workers:' + worker, 'balance', -amount]);
					if (config.payments.assets[asset].dynamicTransferFee && config.payments.assets[asset].minerPayFee) {
					    transferCommands[commandIndex].redis.push(['hincrby', config.coin + ':workers:' + worker, 'balance', -config.payments.assets[asset].transferFee]);
					}
					transferCommands[commandIndex].redis.push(['hincrby', config.coin + ':workers:' + worker, 'paid', amount]);
				    } else {
					transferCommands[commandIndex].redis.push(['hincrby', config.coin + ':workers:' + worker, 'balance-' + asset, -amount]);
					if (config.payments.assets[asset].dynamicTransferFee && config.payments.assets[asset].minerPayFee) {
					    transferCommands[commandIndex].redis.push(['hincrby', config.coin + ':workers:' + worker, 'balance-' + asset, -config.payments.assets[asset].transferFee]);
					}
					transferCommands[commandIndex].redis.push(['hincrby', config.coin + ':workers:' + worker, 'paid-' + asset, amount]);
				    }
				    transferCommands[commandIndex].amount += amount;

				    addresses++;
				    commandAmount += amount;

				    if (config.payments.assets[asset].dynamicTransferFee) {
					transferCommands[commandIndex].rpc.fee = config.payments.assets[asset].transferFee * addresses;
				    }

				    if (addresses >= config.payments.assets[asset].maxAddresses || (config.payments.assets[asset].maxTransactionAmount && commandAmount >= config.payments.assets[asset].maxTransactionAmount) || with_payment_id) {
					commandIndex++;
					addresses = 0;
					commandAmount = 0;
				    }
				}
			    } else {
				let amount = parseInt(payments[worker]);
				if (config.payments.maxTransactionAmount && amount + commandAmount > config.payments.maxTransactionAmount) {
					amount = config.payments.maxTransactionAmount - commandAmount;
				}

				let address = worker;
				let payment_id = null;

				let with_payment_id = false;

				let addr = address.split(config.poolServer.paymentId.addressSeparator);
				if ((addr.length === 1 && utils.isIntegratedAddress(address)) || addr.length >= 2) {
					with_payment_id = true;
					if (addr.length >= 2) {
						address = addr[0];
						payment_id = addr[1];
						payment_id = payment_id.replace(/[^A-Za-z0-9]/g, '');
						if (payment_id.length !== 16 && payment_id.length !== 64) {
							with_payment_id = false;
							payment_id = null;
						}
					}
					if (addresses > 0) {
						commandIndex++;
						addresses = 0;
						commandAmount = 0;
					}
				}

				if (config.poolServer.fixedDiff && config.poolServer.fixedDiff.enabled) {
					addr = address.split(config.poolServer.fixedDiff.addressSeparator);
					if (addr.length >= 2) address = addr[0];
				}

				if (!transferCommands[commandIndex]) {
					transferCommands[commandIndex] = {
						redis: [],
						amount: 0,
						rpc: {
							destinations: [],
							fee: config.payments.transferFee,
							priority: config.payments.priority,
							unlock_time: 0
						}
					};
					if (config.payments.ringSize)
						transferCommands[commandIndex].rpc.ring_size = ringSize;
					else
						transferCommands[commandIndex].rpc.mixin = ringSize;
				}

				transferCommands[commandIndex].rpc.destinations.push({
					amount: amount,
					address: address
				});
				if (payment_id) transferCommands[commandIndex].rpc.payment_id = payment_id;

				transferCommands[commandIndex].redis.push(['hincrby', config.coin + ':workers:' + worker, 'balance', -amount]);
				if (config.payments.dynamicTransferFee && config.payments.minerPayFee) {
					transferCommands[commandIndex].redis.push(['hincrby', config.coin + ':workers:' + worker, 'balance', -config.payments.transferFee]);
				}
				transferCommands[commandIndex].redis.push(['hincrby', config.coin + ':workers:' + worker, 'paid', amount]);
				transferCommands[commandIndex].amount += amount;

				addresses++;
				commandAmount += amount;

				if (config.payments.dynamicTransferFee) {
					transferCommands[commandIndex].rpc.fee = config.payments.transferFee * addresses;
				}

				if (addresses >= config.payments.maxAddresses || (config.payments.maxTransactionAmount && commandAmount >= config.payments.maxTransactionAmount) || with_payment_id) {
					commandIndex++;
					addresses = 0;
					commandAmount = 0;
				}
			    }
			}

			let timeOffset = 0;
			let notify_miners = [];

			let daemonType = config.daemonType ? config.daemonType.toLowerCase() : "default";

			async.filter(transferCommands, function (transferCmd, cback) {
				let rpcCommand = "transfer";
				let rpcRequest = transferCmd.rpc;

				if (daemonType === "bytecoin") {
					rpcCommand = "sendTransaction";
					rpcRequest = {
						transfers: transferCmd.rpc.destinations,
						fee: transferCmd.rpc.fee,
						anonymity: ringSize,
						unlockTime: transferCmd.rpc.unlock_time
					};
					if (transferCmd.rpc.payment_id) {
						rpcRequest.paymentId = transferCmd.rpc.payment_id;
					}
				}

			        if (config.coin == "haven") {
				    rpcCommand = transferCmd.rpc.method;
				    delete rpcRequest.method;
				}

			    log('error', logSystem, 'TRANSFERCMD = %j\nRPCCOMMAND = %j\nRPCREQUEST = %j', [transferCmd, rpcCommand, rpcRequest]);
			    
				apiInterfaces.rpcWallet(rpcCommand, rpcRequest, function (error, result) {
					if (error) {
						log('error', logSystem, 'Error with %s RPC request to wallet daemon %j', [rpcCommand, error]);
						log('error', logSystem, 'Payments failed to send to %j', transferCmd.rpc.destinations);
						cback(false);
						return;
					}

					let now = (timeOffset++) + Date.now() / 1000 | 0;
				        let txHash = daemonType === "bytecoin" ? result.transactionHash : config.coin == "haven" ? result.tx_hash_list.join(",") : result.tx_hash;
					txHash = txHash.replace('<', '')
						.replace('>', '');

					transferCmd.redis.push(['zadd', config.coin + ':payments:all', now, [
						txHash,
						transferCmd.amount,
						transferCmd.rpc.fee,
						ringSize,
						Object.keys(transferCmd.rpc.destinations)
						.length
					].join(':')]);

					let notify_miners_on_success = [];
					for (let i = 0; i < transferCmd.rpc.destinations.length; i++) {
						let destination = transferCmd.rpc.destinations[i];
						if (transferCmd.rpc.payment_id) {
							destination.address += config.poolServer.paymentId.addressSeparator + transferCmd.rpc.payment_id;
						}
						transferCmd.redis.push(['zadd', config.coin + ':payments:' + destination.address, now, [
							txHash,
							destination.amount,
							transferCmd.rpc.fee,
							ringSize
						].join(':')]);

						notify_miners_on_success.push(destination);
					}

					log('info', logSystem, 'Payments sent via wallet daemon %j', [result]);
					redisClient.multi(transferCmd.redis)
						.exec(function (error, replies) {
							if (error) {
								log('error', logSystem, 'Super critical error! Payments sent yet failing to update balance in redis, double payouts likely to happen %j', [error]);
								log('error', logSystem, 'Double payments likely to be sent to %j', transferCmd.rpc.destinations);
								cback(false);
								return;
							}

							for (let m in notify_miners_on_success) {
								notify_miners.push(notify_miners_on_success[m]);
							}

							cback(true);
						});
				});
			}, function (succeeded) {
				let failedAmount = transferCommands.length - succeeded.length;

				for (let m in notify_miners) {
					let notify = notify_miners[m];
					log('info', logSystem, 'Payment of %s to %s', [utils.getReadableCoins(notify.amount), notify.address]);
					notifications.sendToMiner(notify.address, 'payment', {
						'ADDRESS': notify.address.substring(0, 7) + '...' + notify.address.substring(notify.address.length - 7),
						'AMOUNT': utils.getReadableCoins(notify.amount),
					});
				}
				log('info', logSystem, 'Payments splintered and %d successfully sent, %d failed', [succeeded.length, failedAmount]);

				callback(null);
			});

		}

	], function (error, result) {
		setTimeout(runInterval, config.payments.interval * 1000);
	});
}

runInterval();
