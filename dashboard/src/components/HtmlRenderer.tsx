const HtmlRenderer = ({ htmlString }: { htmlString: any }) => {
  return (
    <div
      className="html-content"
      dangerouslySetInnerHTML={{ __html: htmlString }}
    />
  );
};

export default HtmlRenderer;
