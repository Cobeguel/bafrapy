module.exports = {
  directusUrl: process.env.DIRECTUS_URL || 'http://bafrapy-directus:8055',
  directusEmail: process.env.DIRECTUS_ADMIN_EMAIL,
  directusPassword: process.env.DIRECTUS_ADMIN_PASSWORD,
  dumpPath: '/workspace/directus-schema',
  specs: false
};