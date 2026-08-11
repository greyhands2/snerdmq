const { spawn } = require('child_process');

const path = require('path');
const snerd = spawn('cargo', ['run'], { cwd: path.join(__dirname, '..') });

snerd.stdout.on('data', (data) => {
    const lines = data.toString().split('\n').filter(Boolean);
    for (const line of lines) {
        if (line.includes('Compiling') || line.includes('Finished') || line.includes('Running')) continue;
        console.log('[RUST ENGINE] ->', line);
        
        try {
            const msg = JSON.parse(line);
            if (msg.action === 'execute') {
                console.log(`[NODEJS] Executing task ${msg.task_id} with data: ${msg.task_data}...`);
                setTimeout(() => {
                    const result = {
                        action: 'result',
                        task_id: msg.task_id,
                        status: 'success'
                    };
                    console.log('[NODEJS] ->', JSON.stringify(result));
                    snerd.stdin.write(JSON.stringify(result) + '\n');
                }, 500);
            }
        } catch(e) {}
    }
});

snerd.stderr.on('data', (data) => {
    if (!data.toString().includes('Compiling') && !data.toString().includes('Finished')) {
        console.log('STDERR:', data.toString());
    }
});

// Give it a second to boot up, then register and enqueue
setTimeout(() => {
    console.log('[NODEJS] -> Registering handler');
    snerd.stdin.write(JSON.stringify({ action: 'register', task_type: 'send_email' }) + '\n');

    setTimeout(() => {
        console.log('[NODEJS] -> Enqueuing task');
        snerd.stdin.write(JSON.stringify({
            action: 'enqueue',
            task_id: 'test-123',
            task_type: 'send_email',
            task_data: '{"to": "hello@world.com"}',
            max_retries: 3,
            retry_after_hours: 0.001
        }) + '\n');
    }, 500);
}, 1000);

setTimeout(() => {
    console.log('[NODEJS] Test complete, shutting down.');
    snerd.kill();
    process.exit(0);
}, 5000);
