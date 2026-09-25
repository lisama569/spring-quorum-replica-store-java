package com.github.lisama569.springquorumreplicastorejava.web;

import com.github.lisama569.springquorumreplicastorejava.cluster.RejectReason;
import com.github.lisama569.springquorumreplicastorejava.cluster.ReplicationException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.util.Map;

@RestControllerAdvice
public class GlobalExceptionHandler {

    @ExceptionHandler(ReplicationException.class)
    public ResponseEntity<Map<String, Object>> handle(ReplicationException ex) {
        RejectReason reason = ex.getReason();
        HttpStatus status = switch (reason) {
            case KEY_NOT_FOUND -> HttpStatus.NOT_FOUND;
            case CAS_CONFLICT, STALE_TERM, LOG_CONFLICT, DUPLICATE_MISMATCH -> HttpStatus.CONFLICT;
            default -> HttpStatus.SERVICE_UNAVAILABLE;
        };
        return ResponseEntity.status(status).body(Map.of(
                "success", false,
                "rejectReason", reason.name(),
                "requestId", String.valueOf(ex.getRequestId()),
                "term", ex.getTerm(),
                "commitIndex", ex.getCommitIndex(),
                "detail", ex.getMessage()
        ));
    }

    @ExceptionHandler(IllegalArgumentException.class)
    public ResponseEntity<Map<String, Object>> handleBadRequest(IllegalArgumentException ex) {
        return ResponseEntity.badRequest().body(Map.of("success", false, "detail", ex.getMessage()));
    }
}
