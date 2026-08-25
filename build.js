const esbuild = require('esbuild');
const { execFileSync } = require('child_process');

async function build() {
  const watchMode = process.argv.includes('--watch');
  const buildOptions = {
    bundle: true,
    platform: 'node',
    target: 'node20',
    minify: !watchMode,
    sourcemap: true,
    external: ['fastify', 'undici', 'ws'],
  };

  try {
    if (watchMode) {
      const contexts = await Promise.all([
        esbuild.context({
          ...buildOptions,
          entryPoints: ['src/index.ts'],
          outfile: 'dist/index.js',
        }),
        esbuild.context({
          ...buildOptions,
          entryPoints: ['src/plugins/sdk.ts'],
          outfile: 'dist/plugins/sdk.js',
        }),
      ]);
      await Promise.all(contexts.map((context) => context.watch()));
      console.log('👀 Gateway build watch 已启动');

      const shutdown = async () => {
        await Promise.all(contexts.map((context) => context.dispose()));
        process.exit(0);
      };

      process.on('SIGINT', shutdown);
      process.on('SIGTERM', shutdown);
      return;
    }

    await Promise.all([
      esbuild.build({
        ...buildOptions,
        entryPoints: ['src/index.ts'],
        outfile: 'dist/index.js',
      }),
      esbuild.build({
        ...buildOptions,
        entryPoints: ['src/plugins/sdk.ts'],
        outfile: 'dist/plugins/sdk.js',
      }),
    ]);
    execFileSync(process.platform === 'win32' ? 'npx.cmd' : 'npx', [
      'tsc',
      '-p',
      'tsconfig.build.json',
      '--emitDeclarationOnly'
    ], {
      stdio: 'inherit'
    });
    console.log('✅ 构建成功!');
  } catch (error) {
    console.error('❌ 构建失败:', error);
    process.exit(1);
  }
}

build();
