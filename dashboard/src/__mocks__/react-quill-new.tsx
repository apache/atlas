/*
 * Mock for react-quill-new
 */

import React from 'react';

const ReactQuill = React.forwardRef<any, any>(({ value, onChange, ...props }, ref) => {
  return (
    <div data-testid="react-quill-mock" ref={ref}>
      <textarea
        value={value || ''}
        onChange={(e) => onChange && onChange(e.target.value)}
        {...props}
      />
    </div>
  );
});

ReactQuill.displayName = 'ReactQuill';

export default ReactQuill;
