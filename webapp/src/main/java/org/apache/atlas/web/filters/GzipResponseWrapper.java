package org.apache.atlas.web.filters;

import javax.servlet.ServletOutputStream;
import javax.servlet.WriteListener;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpServletResponseWrapper;
import java.io.IOException;
import java.io.OutputStream;
import java.util.zip.GZIPOutputStream;

public class GzipResponseWrapper extends HttpServletResponseWrapper {

    private GZIPServletOutputStream gzipOutputStream;
    private ServletOutputStream outputStream;

    public GzipResponseWrapper(HttpServletResponse response) throws IOException {
        super(response);
    }

    @Override
    public ServletOutputStream getOutputStream() throws IOException {
        if (gzipOutputStream == null) {
            outputStream = super.getOutputStream();
            gzipOutputStream = new GZIPServletOutputStream(outputStream);
        }
        return gzipOutputStream;
    }

    @Override
    public void setContentLength(int len) {
        // Ignore, since content length may change after compression
    }

    public void close() throws IOException {
        if (gzipOutputStream != null) {
            gzipOutputStream.finish();
        }
    }

    private class GZIPServletOutputStream extends ServletOutputStream {
        private final GZIPOutputStream gzipStream;

        public GZIPServletOutputStream(OutputStream output) throws IOException {
            this.gzipStream = new GZIPOutputStream(output);
        }

        @Override
        public void write(int b) throws IOException {
            gzipStream.write(b);
        }

        @Override
        public void flush() throws IOException {
            gzipStream.flush();
        }

        @Override
        public void close() throws IOException {
            gzipStream.close();
        }

        @Override
        public boolean isReady() {
            return true;
        }

        @Override
        public void setWriteListener(WriteListener listener) {
            // Not needed for older Spring versions
        }

        public void finish() throws IOException {
            gzipStream.finish();
        }
    }
}