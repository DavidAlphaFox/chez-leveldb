(library
  (chez-leveldb ffi)
  (export
    chez-leveldb-ffi:major-version
    chez-leveldb-ffi:minor-version
    chez-leveldb-ffi:options-create
    chez-leveldb-ffi:options-set-create-if-mssing
    chez-leveldb-ffi:options-destroy
    chez-leveldb-ffi:open
    chez-leveldb-ffi:close
    chez-leveldb-ffi:create-snapshot
    chez-leveldb-ffi:release-snapshot
    chez-leveldb-ffi:read-options-create
    chez-leveldb-ffi:read-options-destroy
    chez-leveldb-ffi:read-options-set-verify-checksum
    chez-leveldb-ffi:read-options-set-fill-cache
    chez-leveldb-ffi:read-options-set-snapshot
    chez-leveldb-ffi:write-options-create
    chez-leveldb-ffi:write-options-destroy
    chez-leveldb-ffi:write-options-set-sync
    chez-leveldb-ffi:put)

  (import
    (chezscheme)
    (rnrs bytevectors))

  (define lib-init
    (begin
      (case (machine-type)
        [ta6osx (load-shared-object "libleveldb.dylib")]
        [else (load-shared-object "libleveldb.so")])))

  (define-ftype chez-leveldb-ffi:db* void*)
  (define-ftype chez-leveldb-ffi:db** (* chez-leveldb-ffi:db*))
  (define-ftype chez-leveldb-ffi:options* void*)
  (define-ftype chez-leveldb-ffi:write-options* void*)
  (define-ftype chez-leveldb-ffi:read-options* void*)
  (define-ftype chez-leveldb-ffi:snapshot* void*)


  (define (void*->bytevector ptr len)
    (define-ftype byte-array (array 0 unsigned-8))
    (let ([arr (make-ftype-pointer byte-array ptr)]
           [bv  (make-bytevector len)])
      (let loop ((i 0))
        (when (< i len)
          (bytevector-u8-set! bv i (ftype-ref byte-array (i) arr))
          (loop (fx+ 1 i))))
      bv))

  (define (void*->string ptr)
    (if (not (= ptr 0))
      (let* ([strlen (foreign-procedure "strlen" (void*) size_t)]
              [len (strlen ptr)])
        (utf8->string (void*->bytevector ptr len)))))

  (define (alloc-pptr)
    (let ([pptr (foreign-alloc (foreign-sizeof 'void*))])
      (foreign-set! 'void* pptr 0 0)
      pptr))

  (define (action-result pptr)
    (let ([ptr (foreign-ref 'void* pptr 0)])
      (if (= 0 ptr)
        (begin
          (foreign-free pptr)
          (values #t (void)))
        (let ([msg (void*->string ptr)])
          (chez-leveldb-ffi:free ptr)
          (foreign-free pptr)
          (values #f msg)))))

  (define leveldb-open
    (foreign-procedure "leveldb_open" (chez-leveldb-ffi:options* string void*) chez-leveldb-ffi:db*))
  (define leveldb-close
    (foreign-procedure "leveldb_close" (chez-leveldb-ffi:db*) void))
  (define leveldb-put
    (foreign-procedure "leveldb_put" (chez-leveldb-ffi:db* chez-leveldb-ffi:write-options*
                                       u8* size_t u8* size_t void*) void))

  (define chez-leveldb-ffi:major-version
    (foreign-procedure __collect_safe "leveldb_major_version" () int))
  (define chez-leveldb-ffi:minor-version
    (foreign-procedure __collect_safe "leveldb_minor_version" () int))

  (define chez-leveldb-ffi:free
    (foreign-procedure "leveldb_free" (void*) void))

  (define chez-leveldb-ffi:options-create
    (foreign-procedure "leveldb_options_create" () chez-leveldb-ffi:options*))
  (define chez-leveldb-ffi:options-destroy
    (foreign-procedure "leveldb_options_destroy" (chez-leveldb-ffi:options*) void))
  (define chez-leveldb-ffi:options-set-create-if-mssing
    (foreign-procedure "leveldb_options_set_create_if_missing" (chez-leveldb-ffi:options* unsigned-8) void))

  (define (chez-leveldb-ffi:open options path)
    (let* ([pptr (alloc-pptr)]
            [db (leveldb-open options path pptr) ])
      (let-values ([(result msg) (action-result pptr)])
        (if result db (display msg)))))

  (define (chez-leveldb-ffi:close db)
    (if (not (= db 0)) (leveldb-close db)))

  (define chez-leveldb-ffi:create-snapshot
    (foreign-procedure "leveldb_create_snapshot" (chez-leveldb-ffi:db*) chez-leveldb-ffi:snapshot*))
  (define chez-leveldb-ffi:release-snapshot
    (foreign-procedure "leveldb_release_snapshot" (chez-leveldb-ffi:db* chez-leveldb-ffi:snapshot*) void))

  (define chez-leveldb-ffi:read-options-create
    (foreign-procedure "leveldb_readoptions_create" () chez-leveldb-ffi:read-options*))
  (define chez-leveldb-ffi:read-options-destroy
    (foreign-procedure "leveldb_readoptions_destroy" (chez-leveldb-ffi:read-options*) void))
  (define chez-leveldb-ffi:read-options-set-verify-checksum
    (foreign-procedure "leveldb_readoptions_set_verify_checksums" (chez-leveldb-ffi:read-options* unsigned-8) void))
  (define chez-leveldb-ffi:read-options-set-fill-cache
    (foreign-procedure "leveldb_readoptions_set_fill_cache" (chez-leveldb-ffi:read-options* unsigned-8) void))
  (define chez-leveldb-ffi:read-options-set-snapshot
    (foreign-procedure "leveldb_readoptions_set_snapshot" (chez-leveldb-ffi:read-options* chez-leveldb-ffi:snapshot*) void))

  (define chez-leveldb-ffi:write-options-create
    (foreign-procedure "leveldb_writeoptions_create" () chez-leveldb-ffi:write-options*))
  (define chez-leveldb-ffi:write-options-destroy
    (foreign-procedure "leveldb_writeoptions_destroy" (chez-leveldb-ffi:write-options*) void))
  (define chez-leveldb-ffi:write-options-set-sync
    (foreign-procedure "leveldb_writeoptions_set_sync" (chez-leveldb-ffi:write-options* unsigned-8) void))

  (define (chez-leveldb-ffi:put db options key value)
    (let* ([pptr (alloc-pptr)]
            [klen (bytevector-length key)]
            [vlen (bytevector-length value)])
      (leveldb-put db options key klen value vlen pptr)
      (let-values ([(result msg) (action-result pptr)])
        (if result result))))
)
