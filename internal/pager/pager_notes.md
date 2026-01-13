# PAGER design notes

1. **page**: The actual data of some page with a pageid on disk, fixed size
2. **frame**: A fixed preallocated buffer that can have the data of a page loaded in
3. **view**: This is a small preallocated struct that holds a few things:
    - a) Metadata about 

## views

Views contain some data:
- index: basically just "view id" - used for freeing the view at the end etc
- frames: a list of refs to frames we are using in this view, the pager uses this to
"give" a worker thread read-in (or new empty) pages to use. It is a fixed max length (lets 
say 32 frames) and a worker can request to load in or store or create arbitrary numbers at 
once to populate the frame. This can be done after the frame has been initially received by
a worker as well - the worker can load one page, read it, ask for another page, read that, etc,
as long as it doesnt exceed "32" frames/pages or whatever number we choose.
- Ops: This is the struct that gets passed to the file-io system if we need to actually
read/write from disk, this is opaque to the worker thread - if a worker thread asks for a page
the pager checks if the page is already paged into an existing frame, then just will give that
frame, otherwise the pager will populate this Ops struct and submit+wait on the file-io system.

Views are preallocated at the start and put in contiguous arrays, and have
a corresponding "ticket" channel that a worker-threads can take an int-index "ticket" 
out of (or wait on the channel) representing which View they are alloweed to use 
for their work. Once theyre done they just put the ticket back into the channel.

## frames

Frames, however, are more complicated to manage free-ness of. We have two things:
1. Pins - an atomic count of how many threads are currently using the frame. Frames
CANNOT be evicted if the have > 0 pins.
2. Freelist - basically a LRU (or whatever strategy) list that points to which frame
(with 0 pins) should be evicted next when we need to page in another page (and don't have
any "empty" frames, which  would only occur at startup until all our frames have had pages
buffered into them at some time or another).

## cursor

Cursor is the lowest level entity that actually understands the contents of pages - 
the cursor will talk directly to a view and orchestrate prefetches, iteration, etc.

**note**: readasync can just use the op-ch (worker can block on it instead of pager...)
I wish there was some way to encapsulate that better though


## frame eviction locking
very tricky concurrency here
