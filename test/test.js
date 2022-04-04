/**
 * test.js
 *
 * Mocha Test Script
 *
 * node-persistent-queue
 *
 * 18/05/2019
 *
 * Copyright (C) 2019 Damien Clark (damo.clarky@gmail.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


/* eslint no-undef: 0 */

const debug = false ;

import os from 'os' ;
import fs from 'fs' ;
import path from 'path' ;
import {jest} from '@jest/globals' ;
import { PersistentQueue as Queue } from '../index.js' ;
 
describe('Calling Constructor', () => {
	it('should use :memory: if file is empty string', async () => {
		let q = new Queue('') ;
		expect(async () => await q.open()).not.toThrow() ;
	}) ;

	it('should throw when passed a batchSize less than 1', () => {
		expect(() => {
			new Queue({store: ':memory:', batchSize: -1}) ;
		}).toThrowError(Error) ;
	}) ;

	it('should throw when passed a batchSize that is not a number', () => {
		expect(() => {
			new Queue({ store: ':memory:', batchSize: 'text'}) ;
		}).toThrowError(Error) ;
	}) ;
}) ;

describe('Correct queue fifo order', () => {
	let q ;
	beforeAll(done => {
		// Remove previous db3.sqlite (if exists) before creating db anew
		fs.unlink('./test/db3.sqlite', () => {
			q = new Queue({store: './test/db3.sqlite'}) ;
			done() ;
		}) ;
	}) ;

	it('should execute jobs in fifo order', (done) => {
		let sequence = 0 ;
		q.on('next', task => {
			expect(task.job.sequence).toBe(sequence++) ;
			q.done() ;
		}) ;

		q.on('empty', () => {
			q.close() ;
			done() ;
		}) ;

		q.open().then(() => {
			q.start() ;

			for(let i = 0 ; i < 1000 ; ++i) {
				let task = {sequence: i} ;
				q.add(task) ;
			}
		}) ;
	}) ;
}) ;

describe('Search remaining jobs', () => {
	let q ;
	beforeEach(done => {
		q = new Queue({ store: ':memory:', batchSize: 10}) ;
		q.open()
		.then(() => done())
		.catch(err => done(err)) ;
	}) ;

	it('should find first job in the queue', async () => {
		await q.open() ;

		let promises = [] ;
		for(let i = 1 ; i <= 1000 ; ++i) {
			let task = {sequence: i % 501} ;
			promises.push(q.add(task)) ;
		}

		// Wait for all tasks to be added before calling hasJob method to search for it
		try {
			await Promise.all(promises) ;
			for(let i = 1 ; i <= 500 ; ++i)
				expect(await q.getFirstJobId({sequence: i})).toBe(i) ;
			
			await q.close() ;
		}
		catch (err) {
			console.log(err) ;
		}
	}) ;

	it('should find first job in the in-memory queue', done => {
		q.open()
		.then(() => {

			let promises = [] ;
			promises.push(q.add({})) ;
			for(let i = 1 ; i <= 1000 ; ++i) {
				let task = {sequence: i % 501} ;
				promises.push(q.add(task)) ;
			}

			// Grab first job and throw away so in-memory queue is hydrated
			q.on('next', () => {
				q.stop() ;
				q.done() ;
				// Now let's check if all items are
				for(let i = 1 ; i <= 500 ; ++i)
					q.getFirstJobId({sequence: i}).then(id => expect(id).toBe(i+1)) ;
				
				q.close().then(() => done()) ;
			}) ;

			// Wait for all tasks to be added before calling hasJob method to search for it
			Promise.all(promises)
			.then(() =>{
				q.start() ;
			})
			.catch(err =>{
				console.log(err) ;
			}) ;

		}) ;
	}) ;

	it('should find all matching jobs in the queue and in order', done => {
		q.open()
		.then(() => {

			let promises = [] ;
			for(let i = 1 ; i <= 10 ; ++i) {
				let task = {sequence: i % 5} ;
				promises.push(q.add(task)) ;
			}

			// Wait for all tasks to be added before calling hasJob method to search for it
			Promise.all(promises)
			.then(() =>{
				for(let i = 1 ; i <= 5 ; ++i) {
					q.getJobIds({sequence: i % 5}).then((ids) => {
						expect(ids.length).toBe(2) ;
						expect(ids).toEqual(expect.arrayContaining([i, i + 5])) ;
					}) ;
				}
				
				q.close().then(() =>{
					done() ;
				}) ;
			}) ;

		}) ;
	}) ;

	it('should return empty array if job not in queue', done => {
		q.open()
		.then(() => {

			let promises = [] ;
			for(let i = 1 ; i <= 10 ; ++i) {
				let task = {sequence: i} ;
				promises.push(q.add(task)) ;
			}

			// Wait for all tasks to be added before calling hasJob method to search for it
			Promise.all(promises)
			.then(() =>{
				for(let i = 1 ; i <= 5 ; ++i) {
					q.getJobIds({sequence: 100}).then(ids => 
						expect(ids.length).toBe(0)) ;
				}
				
				q.close().then(() =>{
					done() ;
				}) ;
			}) ;

		}) ;
	}) ;

	it('should return null if job not in queue', done => {
		q.open()
		.then(() => {

			let promises = [] ;
			for(let i = 1 ; i <= 10 ; ++i) {
				let task = {sequence: i} ;
				promises.push(q.add(task)) ;
			}

			// Wait for all tasks to be added before calling hasJob method to search for it
			Promise.all(promises)
			.then(() =>{
				for(let i = 1 ; i <= 5 ; ++i) {
					q.getFirstJobId({sequence: 100}).then(id => 
						expect(id).toBeNull()) ;
				}
				
				q.close().then(() =>{
					done() ;
				}) ;
			}) ;

		}) ;
	}) ;

}) ;

describe('Unopened SQLite DB', () => {
	let q = new Queue({store: ':memory:', batchSize: 2}) ;

	it('should throw on calling start() before open is called', () => {
		expect(() => {
			q.start() ;
		}).toThrowError(Error) ;
	}) ;

	it('should throw on calling isEmpty() before open is called', () => {
		expect(() => {
			q.isEmpty() ;
		}).toThrowError(Error) ;
	}) ;

	it('should throw on calling getSqlite3() before open is called', () => {
		expect(() => {
			q.getSqlite3() ;
		}).toThrowError(Error) ;
	}) ;
}) ;

describe('Open Errors', () => {
	it('should reject Promise on no write permissions to db filename', done => {
		let q = new Queue({store: '/usr/cantwritetome', batchSize: 2}) ;
		expect(q.open()).rejects.toThrow() ;
		done() ;
	}) ;
}) ;

describe('Maintaining queue length count', () => {
	it('should count existing jobs in db on open', done => {
		let q = new Queue({store: './test/db2.sqlite'}) ;
		q.open()
		.then(() => {
			expect(q.getLength()).toBe(1) ;
			return q.close() ;
		})
		.then(() => {
			done() ;
		})
		.catch(err => {
			done(err) ;
		}) ;
	}) ;

	it('should count jobs as added and completed', done => {
		let tmpdb = os.tmpdir() + path.sep + process.pid + '.sqlite' ;
		let q = new Queue({store: tmpdb}) ;

		/**
		 * Count jobs
		 * @type {number}
		 */
		let c = 0 ;

		q.on('add', () => {
			expect(q.getLength()).toBe(++c) ;
		}) ;

		q.open()
		.then(() => {
			q.add('1') ;
			q.add('2') ;
			q.add('3') ;

			return q.close() ;
		})
		.then(() => {
			q = new Queue({store: tmpdb}) ;

			return q.open() ;
		})
		.then(() => {
			expect(q.getLength()).toBe(3) ;

			q.on('next', () => {
				expect(q.getLength()).toBe(c--) ;
				q.done() ;
			}) ;

			q.on('empty', () => {
				expect(q.getLength()).toBe(0) ;
				q.close()
				.then(() => {
					fs.unlinkSync(tmpdb) ;
					done() ;
				}) ;
			}) ;

			q.start() ;
		})
		.catch(err => {
			done(err) ;
		}) ;
	}) ;
}) ;

describe('Close Errors', () => {
	let q = new Queue({store: ':memory:'}) ;

	beforeAll(done => {
		q.open()
		.then(() => {
			done() ;
		}) ;
	}) ;

	it('should close properly', () => {
		q.add('1') ;

		expect(q.close()).resolves.not.toThrow() ;
	}) ;
}) ;


describe('Invalid JSON', () => {
	it('should throw on bad json stored in db', done => {
		let q = new Queue({store: './test/db.sqlite', batchSize: 1}) ;
		expect(q.open()).rejects.toThrowError(SyntaxError) ;
		done() ;
	}) ;
}) ;

describe('Emitters', () => {
	let q ;

	beforeEach(done => {
		q = new Queue({store: ':memory:'}) ;
		q.open()
		.then(() => {
			done() ;
		})
		.catch(err => {
			done(err) ;
		}) ;
	}) ;

	afterEach(done => {
		q.close()
		.then(() =>{
			done() ;
		})
		.catch(err => {
			done(err) ;
		}) ;
	}) ;

	it('should emit add', done => {
		q.on('add', job => {
			expect(job.job).toBe('1') ;
			done() ;
		}) ;

		q.add('1') ;
	}) ;

	it('should emit start', done => {
		const s = jest.fn() ;

		q.on('start', s) ;

		q.start() ;

		expect(s).toHaveBeenCalledTimes(1) ;
		expect(q.isStarted()).toBe(true) ;
		done() ;
	}) ;

	it('should emit next when adding after start', done => {
		q.on('next', job => {
			expect(job.job).toBe('1') ;
			// TODO: q.done() ;
			q.done() ;
			done() ;
			q.stop() ;
		}) ;

		q.start() ;
		q.add('1') ;
	}) ;

	it('should emit next when adding before start', done => {
		q.on('next', job => {
			expect(job.job).toBe('1') ;
			q.done() ;
			done() ;
			q.stop() ;
		}) ;

		q.add('1') ;
		q.start() ;
	}) ;

	it('should emit empty', done => {
		let empty = 0 ;
		q.on('empty', () =>{
			// empty should only emit once
			expect(++empty).toBe(1) ;
			expect(q.getLength()).toBe(0) ;
			done() ;
		}) ;

		q.on('next', job => {
			if(debug) console.log(job) ;
			q.done() ;
		}) ;
		q.add('1') ;
		q.add('2') ;
		q.start() ;
	}) ;

	it('3 adds before start should emit 3 nexts', done => {
		let next = 0 ;
		q.on('empty', () =>{
			expect(next).toBe(3) ;
			expect(q.getLength()).toBe(0) ;
			done() ;
		}) ;

		q.on('next', job => {
			if(debug) console.log(job) ;
			++next ;
			q.done() ;
		}) ;
		q.add('1') ;
		q.add('2') ;
		q.add('3') ;
		q.start() ;
	}) ;

	it('should add 3 jobs and after start should emit 3 nexts', done => {
		let next = 0 ;
		q.on('empty', () =>{
			expect(next).toBe(3) ;
			expect(q.getLength()).toBe(0) ;
			done() ;
		}) ;

		q.on('next', job => {
			if(debug) console.log(job) ;
			++next ;
			q.done() ;
		}) ;
		q.start() ;
		q.add('1') ;
		q.add('2') ;
		q.add('3') ;
	}) ;

	it('should start in middle of 3 adds and should emit 3 nexts', done => {
		let next = 0 ;
		q.on('empty', () =>{
			expect(next).toBe(3) ;
			expect(q.getLength()).toBe(0) ;
			done() ;
		}) ;

		q.on('next', job => {
			if(debug) console.log(job) ;
			++next ;
			q.done() ;
		}) ;
		q.add('1') ;
		q.add('2') ;
		q.start() ;
		q.add('3') ;
	}) ;

	it('should emit stop', done => {
		let stop = 0 ;
		q.on('stop', () =>{
			expect(++stop).toBe(1) ;
			expect(q.isStarted()).toBe(false) ;
			done() ;
		}) ;

		q.on('empty', () =>{
			q.stop() ;
		}) ;

		q.on('next', job => {
			if(debug) console.log(job) ;
			q.done() ;
		}) ;
		q.add('1') ;
		q.add('2') ;
		q.start() ;
		q.add('3') ;
		q.add('4') ;
	}) ;

	it('should emit open', done => {
		let q1 = new Queue({store: ':memory:'}) ;
		let open = 0 ;
		q1.on('open', () => {
			expect(++open).toBe(1) ;
			expect(q1.isOpen()).toBe(true) ;
			q1.close()
			.then(() => {
				done() ;
			}) ;
		}) ;
		q1.open() ;
	}) ;

	it('should emit close', done => {
		let q1 = new Queue({store: ':memory:'}) ;
		let close = 0 ;
		q1.on('close', () => {
			expect(++close).toBe(1) ;
			expect(q1.isOpen()).toBe(false) ;
		}) ;
		q1.open()
		.then(() => {
			return q1.close() ;
		})
		.then(() => {
			done() ;
		}) ;
	}) ;
}) ;

describe('Delay between tasks execution', () => {
	it('shoud be interval between previous task finish and next task start', (done) => {
		const afterProcessDelay = 50 ;

		const q = new Queue({store: ':memory:', afterProcessDelay}) ;
		const delays = [] ;
		let delay_start ;

		q.on('done', () =>{
			delay_start = performance.now() ;
		}) ;

		q.on('next', () => {
			delays.push(performance.now() - delay_start) ;
			q.done() ;
		}) ;

		q.on('empty', () => {
			delays.shift() ;
			
			const average = Math.trunc(delays.reduce((acc, val) => acc+val)/delays.length) ;
			// It depends on machine CPU load, so it might fail
			expect(average).toBe(afterProcessDelay) ;

			q.close().then(() => done()) ;
		}) ;

		q.open().then(() => {
			q.start() ;

			delay_start = performance.now() ;
			for(let i = 0 ; i < 10 ; ++i) {
				let task = {sequence: i} ;
				q.add(task) ;
			}
		}) ;

	}) ;
}) ;