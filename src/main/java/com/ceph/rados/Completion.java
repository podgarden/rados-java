package com.ceph.rados;

import static com.ceph.rados.Library.rados;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;

import com.ceph.rados.exceptions.RadosException;
import com.sun.jna.Callback;
import com.sun.jna.Pointer;
import com.sun.jna.ptr.PointerByReference;

public class Completion extends RadosBase implements Closeable {
	// Static members
	private static Map<Integer,Completion> completionMap = new HashMap<>();
	private static int nextCompletionId = 1;
	
	// Callback support
	private static Callback completeCallback = new Callback() {
		@SuppressWarnings("unused")
		public void callback(Pointer completionPointer, Pointer callbackContext) throws RadosException {
			final int completionId = callbackContext.getInt(0);
			Completion completion;
			synchronized (completionMap) {
				completion = completionMap.get(completionId);
			}
			
			// If the completion has not been closed yet, call the handler.
			if (completion != null)
				completion.onComplete();
		}
	};
	private static Callback safeCallback = new Callback() {
		@SuppressWarnings("unused")
		public void callback(Pointer completionPointer, Pointer callbackContext) throws RadosException {
			final int completionId = callbackContext.getInt(0);
			Completion completion;
			synchronized (completionMap) {
				completion = completionMap.get(completionId);
			}

			// If the completion has not been closed yet, call the handler.
			if (completion != null)
				completion.onSafe();
		}
	};

	// Instance members
	private Pointer pointer;
	private int completionId;
	
    /**
     * Constructs a completion to use with asynchronous operations.
     * <p>
     * The complete and safe callbacks correspond to operations being acked and committed, respectively.
     * The callbacks are called in order of receipt, so the safe callback may be triggered before the complete callback, and vice versa.
     * This is affected by journalling on the OSDs.
     * <p>
     * NOTE: Read operations only get a complete callback.
     * 
	 * @param notifyOnComplete If true, onComplete() is called when the operation is in memory on all replicas
	 * @param notifyOnSafe If true, onSafe() is called when the operation is on stable storage on all replicas
     * @throws RadosException 
     */
	public Completion(final boolean notifyOnComplete, final boolean notifyOnSafe) throws RadosException {
		super();
        final PointerByReference pointerByReference = new PointerByReference();
		if (notifyOnComplete || notifyOnSafe) {
			// Record this object in the global completion map so that it can be accessed from the callback handlers. 
			synchronized (completionMap) {
				completionId = nextCompletionId++;
				if (completionId <= 0) {
					completionId = 1;
					nextCompletionId = 2;
				}
				completionMap.put(completionId, this);
			}
			
			// Create the completion object.
			handleReturnCode(new Callable<Integer>() {
				@Override
				public Integer call() throws Exception {
					return rados.rados_aio_create_completion(
							Pointer.createConstant(completionId), 
							notifyOnComplete?completeCallback:null, 
							notifyOnSafe?safeCallback:null,
							pointerByReference
					);
				}
			},
			"Failed to create completion"
		);
			
		} else {
			handleReturnCode(new Callable<Integer>() {
					@Override
					public Integer call() throws Exception {
						return rados.rados_aio_create_completion(null, null, null, pointerByReference);
					}
				},
				"Failed to create completion"
			);
		}
        pointer = pointerByReference.getValue();
	}
	
	/**
	 * Block until the operation completes.  This means it is in memory on all replicas.
	 * @throws RadosException
	 */
	public void waitForComplete() throws RadosException {
        handleReturnCode(new Callable<Integer>() {
	        	@Override
	            public Integer call() throws Exception {
	                return rados.rados_aio_wait_for_complete(getPointer());
	            }
	        },
	        "Failed to wait for AIO completion"
	    );		
	}

	/**
	 * Override this function to implement callback handling.  If notifyOnSafe is true, this function is called when
	 * the operation is in memory on all replicas. 
	 */
	public void onComplete() {
	}
	
	/**
	 * Override this function to implement callback handling.  If notifyOnComplete is true, this function is called when
	 * the operation is on stable storage on all replicas. 
	 */
	public void onSafe() {
	}
	
    /**
     * Release a completion
     * <p>
     * Call this when you no longer need the completion. It may not be freed immediately if the operation is not acked and committed.
     */
	@Override
	public void close() throws IOException {
		rados.rados_aio_release(getPointer());
		if (completionId > 0) {
			synchronized (completionMap) {
				completionMap.remove(completionId);
			}
			completionId = 0;
		}
	}

	public Pointer getPointer() {
		return pointer;
	}
}
