import '@testing-library/dom'

// jsdom doesn't implement scrollIntoView
window.HTMLElement.prototype.scrollIntoView = function() {}
