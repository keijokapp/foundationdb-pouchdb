import base from '@arbendium/eslint-config-base';

export default [
	...base,
	{
		files: ['eslint.config.js', 'test'],
		rules: {
			'import/no-extraneous-dependencies': ['error', { devDependencies: true }]
		}
	},
	{
		rules: {
			'no-underscore-dangle': 'off'
		}
	}
];
