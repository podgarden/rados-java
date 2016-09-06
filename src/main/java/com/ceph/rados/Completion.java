package com.ceph.rados;

import static com.ceph.rados.Library.rados;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.Callable;

import com.ceph.rados.exceptions.RadosException;
import com.sun.jna.Callback;
import com.sun.jna.Pointer;
import com.sun.jna.ptr.PointerByReference;

public class Completion extends RadosBase implements Closeable {
	private Pointer pointer;
	
    /**
     * Constructs a completion to use with asynchronous operations.
     * <p>
     * The complete and safe callbacks correspond to operations being acked and committed, respectively.
     * The callbacks are called in order of receipt, so the safe callback may be triggered before the complete callback, and vice versa.
     * This is affected by journalling on the OSDs.
     * <p>
     * NOTE: Read operations only get a complete callback.
     * 
     * @param callbackContext application-defined data passed to the callback functions
     * @param callbackComplete the function to be called when the operation is in memory on all replicas
     * @param callbackSafe the function to be called when the operation is on stable storage on all replicas
     * @param pointer where to store the completion
     * @return 0
     * @throws RadosException 
     */
	public Completion(final Pointer callbackContext, final Callback callbackComplete, final Callback callbackSafe) throws RadosException {
		super();
        final PointerByReference pointerByReference = new PointerByReference();
        handleReturnCode(new Callable<Integer>() {
	            @Override
	            public Integer call() throws Exception {
	                return rados.rados_aio_create_completion(callbackContext, callbackComplete, callbackSafe, pointerByReference);
	            }
	        },
	        "Failed to create completion"
	    );
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
     * Release a completion
     * <p>
     * Call this when you no longer need the completion. It may not be freed immediately if the operation is not acked and committed.
     */
	@Override
	public void close() throws IOException {
		rados.rados_aio_release(getPointer());
	}

	public Pointer getPointer() {
		return pointer;
	}
	
	/* TODO
	private static class CallbackHandler implements Callback {
		CompletionCallback completionCallback;
		
		public void callback(Pointer completion, Pointer callbackContext) {
			completionCallback.callback(completion, callbackContext);
		}
	}

	public static interface CompletionCallback {
		public void callback(Completion completion, Pointer callbackContext);
	}
	*/
}
