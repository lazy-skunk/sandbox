const form = document.getElementById('chat-form');
const message = document.getElementById('message');
const button = document.getElementById('send');
const statusMessage = document.getElementById('status');
const answer = document.getElementById('answer');
const trace = document.getElementById('trace');

form.addEventListener('submit', async (event) => {
  event.preventDefault();
  button.disabled = true;
  statusMessage.textContent = '処理中…';
  answer.textContent = '';
  trace.textContent = '';

  try {
    const response = await fetch('/chat', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({message: message.value})
    });
    const data = await response.json();
    answer.textContent = data.answer ?? data.error;
    trace.textContent = JSON.stringify(data.trace, null, 2);
    statusMessage.textContent = response.ok && !data.trace.some(entry => entry.tool)
      ? 'MCP ツールは呼び出されませんでした。'
      : '';
  } catch {
    statusMessage.textContent = '通信に失敗しました。';
  } finally {
    button.disabled = false;
  }
});
