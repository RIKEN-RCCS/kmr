;; fugaku-nodeid.scm (in R7RS). (2021-06-04)

;; This calculates a node-ID from a Tofu coordinate on Fugaku.  Run it
;; with "guile".  Example: (nodeid '(18 16 2 0 0 0)).

;; A node position by a Tofu coordinate (X,Y,Z,a,b,c) is used in the
;; network.  In contrast, a node-ID, which is a 32-bit integer in
;; hex-decial, is used in the system software of Fugaku.  This code
;; calcualtes a node-ID from a Tofu coordinate.
;;
;; It is guessed that the node-IDs are assigned hierarchically.
;; Fugaku has dimension sizes of (24,23,24,2,3,2).
;;
;; (1) The top-level consists of 12 blocks.  Each block has
;; 96x12x12=13824 nodes, except for the last block which has 6912
;; nodes.  The blocks are ordered by <Y/2> and scattered in the range
;; (-,[0:2:22],-,-,-,-).  The remaining index in a subblock is
;; Y'=(Y&1).  Note that the last block consists of Y'=0 block only.
;;
;; (2) The 2nd-level consists of 12 blocks, with each block having
;; 12x96 nodes.  The block are ordered by <X/2> and scattered in the
;; range ([0:2:22],-,-,-,-,-).  The remaining index in a subblock is
;; X'=(X&1).  (Each block is further divided to 4x3 blocks).
;;
;; (3) The 3rd-level consists of 12 blocks, with each block having 96
;; nodes.  The block are ordered by <Z4/2,Y',Z4&1> where Z4=Z/4, and
;; occupy the subrange (-,2,6,-,-,-).  The remaining index in a
;; subblock is Z'=(Z&3).
;;
;; (4) The 4th-level consists of 2 blocks, with each block having 48
;; nodes.  The blocks are ordered by <X'> and occupy the subrange
;; (2,-,-,-,-,-).
;;
;; (5) The 5th-level consists of blocks of 48 nodes.  The nodes are
;; ordered by <b,c,(X',Z',a)> and occupy the subrange (-,-,4,2,3,2).
;; The (X',Z',a) pairs are ordered by: when X'=0: (-,2,0) < (-,1,1) <
;; (-,1,0) < (-,0,0) < (-,3,0) < (-,0,1) < (-,2,1) < (-,3,1); and when
;; X'=1: (-,2,1) < (-,1,0) < (-,1,1) < (-,0,0) < (-,2,0) < (-,3,0) <
;; (-,0,1) < (-,3,1).
;;
;; (6) The indices in the hierarchy are distributed sparsely into
;; three bit-fields in a 32-bit node ID.

(define quiet-for-check #f)

(define (assert c)
  (when (not c)
    (error "assert")))

(define (za-index x z a)
  (begin
    (assert (and (<= 0 x 1) (<= 0 z 3) (<= 0 a 1)))
    (if (= x 0)
	(case (+ (* z 10) a)
	  ((20) 0)
	  ((11) 1)
	  ((10) 2)
	  ((00) 3)
	  ((30) 4)
	  ((01) 5)
	  ((21) 6)
	  ((31) 7)
	  (else (error "za-index")))
	(case (+ (* z 10) a)
	  ((21) 0)
	  ((10) 1)
	  ((11) 2)
	  ((00) 3)
	  ((20) 4)
	  ((30) 5)
	  ((01) 6)
	  ((31) 7)
	  (else (error "za-index"))))))

(define (node-id x y z a b c)
  (begin
    ;; (x,y,z,a,b,c) range: (24,23,24,2,3,2).
    (assert (and (<= 0 x 23) (<= 0 y 22) (<= 0 z 23)
		 (<= 0 a 1) (<= 0 b 2) (<= 0 c 1)))
    ;; [top-level (x,y,z,a,b,c)]
    (let ((index1 (floor/ y 2))
	  (y' (logand y 1))
	  (last-block? (= (floor/ y 2) 11)))
      ;; [2nd-level (x,y',z,a,b,c)]
      (let ((index2 (floor/ x 2))
	    (x' (logand x 1)))
	;; [3rd-level (x',y',z,a,b,c)]
	(let ((z4 (floor/ z 4))
	      (z-scaling (if (not last-block?) 4 2)))
	  (let ((index3 (+ (* (floor/ z4 2) z-scaling) (* y' 2) (logand z4 1)))
		(z' (logand z 3)))
	    ;; [4th-level (x',-,z',a,b,c)]
	    (let ((index4 x'))
	      ;; [5th-level (-,-,z',a,b,c)]
	      (let ((za (za-index x' z' a)))
		(let ((index5 (+ (* b 16) (* c 8) za))
		      (index2-scaling (if (not last-block?) 72 36)))
		  (let ((slot2 (+ (* index1 4) (floor/ index2 3)))
			(slot1 (+ (* (modulo index2 3) index2-scaling)
				  (* index3 6)
				  (* index4 3)
				  (floor/ index5 16)))
			(slot0 (modulo index5 16)))
		    (begin
		      (when (not quiet-for-check)
			(format #t
				"index [1]=~s [2]=~s [3]=~s [4]=~s [5]=~s\n"
				index1 index2 index3 index4 index5))
		      (+ #x01010001
			 (* slot2 #x01000000)
			 (* slot1 #x00010000)
			 (* slot0 #x00000001)))))))))))))

(define (nodeid xyzabc)
  (let ((id (apply node-id xyzabc)))
    (begin
      (when (not quiet-for-check)
	(format #t "~8,'0x\n" id))
      id)))

(define (check pairs)
  "Checks on an id and position pair.  Run it by mapping on a list:
   '((#x01010005 (0 0 3 0 0 0) ...) ...)."
  (let ((id (car pairs))
	(pos (cadr pairs)))
    (assert (equal? id (nodeid pos)))))
